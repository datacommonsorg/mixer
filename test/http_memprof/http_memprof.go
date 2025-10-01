// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// this code is a utility to monitor an already running mixer instance, and such
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	pbs "github.com/datacommonsorg/mixer/internal/proto/service"
	pbv1 "github.com/datacommonsorg/mixer/internal/proto/v1"
	"github.com/google/pprof/profile"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	grpcAddr          = flag.String("grpc_addr", "127.0.0.1:12345", "Address of grpc server.")
	profAddr          = flag.String("prof_addr", "127.0.0.1:6060", "Address of HTTP profile server.")
	outFolderParent   = flag.String("out_folder", "http_memprof_out", "Folder to store output of memory profiles over HTTP")
	resultCsvFilename = "results.csv"
)

func readProfile(filename string) (*profile.Profile, error) {
	reader, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	prof, err := profile.Parse(reader)
	return prof, err
}

// GetTotalSpaceAllocFromProfile reads the pprof compatible report file at the
// given path and returns the total allocated space it recorded. Assumes that
// the memory profile sample_index for alloc_space is 1
func GetTotalSpaceAllocFromProfile(filename string) (int64, error) {
	// Modified from https://github.com/google/pprof/blob/c488b8fa1db3fa467bf30beb5a1d6f4f10bb1b87/internal/report/report.go
	// =======================
	// computeTotal computes the sum of the absolute value of all sample values.
	// If any samples have label indicating they belong to the diff base, then the
	// total will only include samples with that label.
	computeTotal := func(prof *profile.Profile, value func(v []int64) int64) int64 {
		var total, diffTotal int64
		for _, sample := range prof.Sample {
			var v int64
			v = value(sample.Value)
			if v < 0 {
				v = -v
			}
			total += v
			if sample.DiffBaseSample() {
				diffTotal += v
			}
		}
		if diffTotal > 0 {
			total = diffTotal
		}
		return total
	}
	// sample index of alloc_space in heap profiles from http/net/pprof
	const allocSpaceSampleIndex = 1

	prof, err := readProfile(filename)
	if err != nil {
		return 0, err
	}
	valuef := func(v []int64) int64 {
		return v[allocSpaceSampleIndex]
	}
	total := computeTotal(prof, valuef)
	return total, nil
}

func bytesToMegabytes(bytes int64) float64 {
	return float64(bytes) / (1024 * 1024)
}

func saveProfile(outFolder string) error {
	outputFlag := fmt.Sprintf("-output=%v", outFolder)
	heapProfileHTTPPath := fmt.Sprintf("%v/debug/pprof/heap?gc=1", *profAddr)
	cmd := exec.Command("go", "tool", "pprof", outputFlag, "-proto", heapProfileHTTPPath)
	err := cmd.Run()
	return err
}

// MemoryProfileResult holds the results from one gRPC call that was
// profiled for its total memory allocation
type MemoryProfileResult struct {
	profileKey     string
	allocMB        float64
	responseLength int
}

// RunWithProfile runs the function f identified by name key. f is expected to
// make a request to the mixer server and return a string representing the API
// response gathered within f. Returns a MemoryProfileResult including the
// memory allocated at the mixer server over the lifetime of f.
func RunWithProfile(outFolder string, key string, f func() (string, error)) (*MemoryProfileResult, error) {
	profileLocationTemplate := outFolder + "/go_http_memprof.%v.%v.pb"

	profBefore := fmt.Sprintf(profileLocationTemplate, "before", key)
	profAfter := fmt.Sprintf(profileLocationTemplate, "after", key)

	err := saveProfile(profBefore)
	if err != nil {
		return nil, err
	}
	respStr, err := f()
	if err != nil {
		return nil, err
	}
	err = saveProfile(profAfter)
	if err != nil {
		return nil, err
	}

	allocBefore, err := GetTotalSpaceAllocFromProfile(profBefore)
	if err != nil {
		return nil, err
	}
	allocAfter, err := GetTotalSpaceAllocFromProfile(profAfter)
	if err != nil {
		return nil, err
	}

	allocDiff := allocAfter - allocBefore
	allocDiffMb := bytesToMegabytes(allocDiff)

	fmt.Printf("%v used %.2f MB and returned a response of length %d\n", key, allocDiffMb, len(respStr))
	return &MemoryProfileResult{
		profileKey:     key,
		allocMB:        allocDiffMb,
		responseLength: len(respStr),
	}, nil

	// NOTE: Another approach here would be to use go tool pprof to compute a
	// profile with "substraction", where the profile consists of the
	// differences with the "base profile". The numerical diff can be read from
	// that "diff profile" the same way we read the total memory allocated in
	// the "before" and "after" profiles currently.
	// To achieve this, one could do something like the following:
	//////////
	// profDiff := fmt.Sprintf(profileLocationTemplate, "diff", key)
	// outputFlag := fmt.Sprintf("-output=%v", profDiff)
	// // error-checking omitted from the following
	// cmd := exec.Command("go", "tool", "pprof", "-proto", outputFlag, "-sample_index=alloc_space", "-base", profBefore, profAfter)
	// allocDiffMb := bytesToMegabytes(GetTotalSpaceAllocFromProfile(profDiff)
	//////////
}

func writeResultsToCsv(results []*MemoryProfileResult, outputPath string) {
	f, err := os.Create(outputPath)
	if err != nil {
		log.Fatal(err)
	}
	//nolint:errcheck // TODO: Fix pre-existing issue and remove comment.
	defer f.Close()

	header := "ProfileKey,AllocatedMemoryMB,ResponseLength"
	//nolint:errcheck // TODO: Fix pre-existing issue and remove comment.
	fmt.Fprintln(f, header)

	for _, result := range results {
		//nolint:errcheck // TODO: Fix pre-existing issue and remove comment.
		fmt.Fprintf(f, "%v,%.2f,%d\n", (*result).profileKey, (*result).allocMB, (*result).responseLength)
	}
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	flag.Parse()

	// Create the output folder, if it doesn't exist.
	// Form the actual output folder as <flag_value>/<unix_time> to preserve
	// data from previous runs
	now := time.Now()
	outFolder := filepath.Join(*outFolderParent, strconv.FormatInt(now.Unix(), 10))
	fmt.Println("Writing results to path:", outFolder)
	err := os.MkdirAll(outFolder, 0o0755)
	if err != nil {
		log.Fatalf("could not create temp directory at %v: %v", outFolder, err)
	}

	// Set up a connection to the server.
	// gRPC server start listening last, so if we have connected to gRPC, we can
	// assume that the pprof HTTP handlers is also on.
	fmt.Printf("Attempting to make a connection to the gRPC server at %v\n", *grpcAddr)
	conn, err := grpc.NewClient(*grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100000000 /* 100M */)),
	)
	if err != nil {
		log.Fatalf("Could not create client: %v", err)
	}
	// Wait up to 30s for the connection to be ready.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		s := conn.GetState()
		if s == connectivity.Ready {
			break
		}
		if s == connectivity.Idle {
			conn.Connect()
		}
		if !conn.WaitForStateChange(ctx, s) {
			log.Fatalf("Could not connect to %s: timed out. Last state: %s", *grpcAddr, s)
		}
	}
	slog.Info("Connected to gRPC succesfully")

	//nolint:errcheck // TODO: Fix pre-existing issue and remove comment.
	defer conn.Close()
	c := pbs.NewMixerClient(conn)

	// TODO: move the definition of requests to make to a config file
	var profileResults []*MemoryProfileResult
	var count int64
	for _, allFacets := range []bool{true, false} {
		for _, r := range []struct {
			entityType   string
			linkedEntity string
			variables    []string
		}{
			{
				"County",
				"country/USA",
				[]string{"dummy", "Median_Age_Person_AmericanIndianOrAlaskaNativeAlone"},
			},
			{
				"City",
				"country/USA",
				[]string{"Median_Age_Person_AmericanIndianOrAlaskaNativeAlone"},
			},
			{
				"Country",
				"Earth",
				[]string{"Median_Age_Person"},
			},
			{
				"EpaReportingFacility",
				"geoId/06",
				[]string{"Annual_Emissions_GreenhouseGas_NonBiogenic"},
			},
			{
				"AdministrativeArea2",
				"country/FRA",
				[]string{"Count_Person"},
			},
		} {
			funcKey := fmt.Sprintf("BulkObservationsSeriesLinked_%d", count)
			funcToProfile := func() (string, error) {
				req := &pbv1.BulkObservationsSeriesLinkedRequest{
					Variables:      r.variables,
					EntityType:     r.entityType,
					LinkedEntity:   r.linkedEntity,
					LinkedProperty: "containedInPlace",
					AllFacets:      allFacets,
				}
				resp, err := c.BulkObservationsSeriesLinked(ctx, req)
				if err != nil {
					return "", err
				}
				respStr := resp.String()
				return respStr, nil
			}
			result, err := RunWithProfile(outFolder, funcKey, funcToProfile)
			if err != nil {
				log.Fatal(err)
			}
			profileResults = append(profileResults, result)
			count++
		}
	}

	writeResultsToCsv(profileResults, filepath.Join(outFolder, resultCsvFilename))
}
