// Copyright 2019 Google LLC
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

package main

import (
	"context"
	"flag"
	"log"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	pb "github.com/datacommonsorg/mixer/internal/proto"
	"google.golang.org/grpc"
)


var (
	addr = flag.String("addr", "127.0.0.1:12345", "Address of grpc server.")
	temp_path = flag.String("temp_path", "http_memprof_out", "Folder to store temporary output of memory profiles over HTTP")
)

func save_profile(prof_identifier string) {
	output_location_flag := fmt.Sprintf("-output=%v/go_http_memprof.%v.pb", *temp_path, prof_identifier)
	cmd := exec.Command("go", "tool", "pprof", output_location_flag, "-proto", "http://localhost:6060/debug/pprof/heap?gc=1")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		fmt.Println(fmt.Sprint(err) + ": " + stderr.String())
		return
	}
}

func compute_memory_profile_diff (f_identifier string) {
	profile_location_template := *temp_path + "/go_http_memprof.%v.%v.pb"
	profile_before := fmt.Sprintf(profile_location_template, "before", f_identifier)
	profile_after := fmt.Sprintf(profile_location_template, "after", f_identifier)

	cmd := exec.Command("go", "tool", "pprof", "-top", "-nodecount=10", "-sample_index=alloc_space", "-base", profile_before, profile_after)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		fmt.Println(fmt.Sprint(err) + ": " + stderr.String())
		return
	}
	fmt.Println(stdout.String())
}

func run_with_profile (f_identifier string, f func()) {
	save_profile("before." + f_identifier)
	f()
	save_profile("after." + f_identifier)
	compute_memory_profile_diff(f_identifier)
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	flag.Parse()

	// Ensure the temp output path exists
	err := os.MkdirAll(*temp_path, os.ModePerm)
	if err != nil {
		log.Fatalf("could not create temp directory at %v: %v", temp_path, err)
	}

	// Set up a connection to the server.
	conn, err := grpc.Dial(*addr,
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100000000 /* 100M */)),
	)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewMixerClient(conn)
	ctx := context.Background()

	funcs_to_profile := make(map[string]func())

	funcs_to_profile["BulkObservationsSeriesLinked_USA"] = func(){
			req := &pb.BulkObservationsSeriesLinkedRequest{
				Variables:      []string{"Median_Age_Person_AmericanIndianOrAlaskaNativeAlone"},
				EntityType:     "City",
				LinkedEntity:   "country/USA",
				LinkedProperty: "containedInPlace",
				AllFacets:      false,
			}
			_, err := c.BulkObservationsSeriesLinked(ctx,req)
			if err != nil {
				log.Fatalf("could not run BulkObservationsSeriesLinked: %s", err)
			}
			fmt.Printf("BulkObservationsSeriesLinked returned succesfully\n")
			// Commenting for now because output floods screen
			// fmt.Printf("%d\n", proto.MarshalTextString(r))
		}

		for key, f := range funcs_to_profile {
			run_with_profile(key, f)
		}


	return;
}
