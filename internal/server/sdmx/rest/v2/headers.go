// Copyright 2026 Google LLC
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

package restv2

import (
	"mime"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DataResponseFormat int

const (
	DataResponseFormatJSONStat DataResponseFormat = iota
	DataResponseFormatCSV
)

type AvailabilityResponseFormat int

const (
	AvailabilityResponseFormatStructureJSON AvailabilityResponseFormat = iota
)

// Accept media constants are request negotiation tokens, not emitted Content-Type values.
const (
	acceptMediaSDMXDataCSV       = "application/vnd.sdmx.data+csv"
	acceptMediaSDMXDataJSON      = "application/vnd.sdmx.data+json"
	acceptMediaSDMXStructureJSON = "application/vnd.sdmx.structure+json"
	acceptMediaSDMXStructureXML  = "application/vnd.sdmx.structure+xml"
	acceptMediaTextCSV           = "text/csv"
	acceptMediaAny               = "*/*"
	acceptParamVersion           = "version"
	acceptParamQ                 = "q"
	acceptVersion2               = "2.0.0"
)

// DataResponseFormatFromAccept selects the SDMX data response format.
func DataResponseFormatFromAccept(accept []string) (DataResponseFormat, error) {
	for _, value := range accept {
		format, found, err := dataResponseFormatFromAccept(value)
		if err != nil {
			return DataResponseFormatCSV, err
		}
		if found {
			return format, nil
		}
	}
	return DataResponseFormatCSV, nil
}

// ValidateDataAccept rejects SDMX wire formats that are not implemented yet.
func ValidateDataAccept(accept []string) error {
	_, err := DataResponseFormatFromAccept(accept)
	return err
}

// AvailabilityResponseFormatFromAccept selects the SDMX availability response format.
func AvailabilityResponseFormatFromAccept(accept []string) (AvailabilityResponseFormat, error) {
	foundAccept := false
	for _, value := range accept {
		foundAccept = true
		format, found, err := availabilityResponseFormatFromAccept(value)
		if err != nil {
			return AvailabilityResponseFormatStructureJSON, err
		}
		if found {
			return format, nil
		}
	}
	if !foundAccept {
		return AvailabilityResponseFormatStructureJSON, nil
	}
	return AvailabilityResponseFormatStructureJSON, nil
}

func dataResponseFormatFromAccept(value string) (DataResponseFormat, bool, error) {
	for _, item := range strings.Split(value, ",") {
		mediaType, params, err := mime.ParseMediaType(strings.TrimSpace(item))
		if err != nil {
			continue
		}
		switch strings.ToLower(mediaType) {
		case acceptMediaSDMXDataCSV, acceptMediaTextCSV:
			if err := validateCSVAcceptParams(params); err != nil {
				return DataResponseFormatJSONStat, true, err
			}
			return DataResponseFormatCSV, true, nil
		case acceptMediaSDMXDataJSON:
			return DataResponseFormatJSONStat, true, status.Error(codes.Unimplemented, "SDMX JSON responses are not implemented yet")
		}
	}
	return DataResponseFormatJSONStat, false, nil
}

func availabilityResponseFormatFromAccept(value string) (AvailabilityResponseFormat, bool, error) {
	var firstErr error
	for _, item := range strings.Split(value, ",") {
		mediaType, params, err := mime.ParseMediaType(strings.TrimSpace(item))
		if err != nil {
			continue
		}
		mediaType = strings.ToLower(mediaType)
		switch mediaType {
		case acceptMediaAny:
			return AvailabilityResponseFormatStructureJSON, true, nil
		case acceptMediaSDMXStructureJSON:
			if err := validateStructureJSONAcceptParams(params); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			return AvailabilityResponseFormatStructureJSON, true, nil
		case acceptMediaSDMXStructureXML:
			if firstErr == nil {
				firstErr = status.Error(codes.Unimplemented, "SDMX structure XML responses are not implemented yet")
			}
		case acceptMediaSDMXDataCSV, acceptMediaSDMXDataJSON:
			if firstErr == nil {
				firstErr = status.Errorf(codes.Unimplemented, "SDMX availability response media type %q is not implemented yet", mediaType)
			}
		default:
			if firstErr == nil {
				firstErr = status.Errorf(codes.Unimplemented, "SDMX availability response media type %q is not implemented yet", mediaType)
			}
		}
	}
	if firstErr != nil {
		return AvailabilityResponseFormatStructureJSON, true, firstErr
	}
	return AvailabilityResponseFormatStructureJSON, false, nil
}

func validateCSVAcceptParams(params map[string]string) error {
	for key, value := range params {
		switch strings.ToLower(key) {
		case acceptParamVersion:
			if value != acceptVersion2 {
				return status.Errorf(codes.Unimplemented, "SDMX CSV version %q is not implemented yet", value)
			}
		case acceptParamQ:
			continue
		default:
			return status.Errorf(codes.Unimplemented, "SDMX CSV response option %q is not implemented yet", key)
		}
	}
	return nil
}

func validateStructureJSONAcceptParams(params map[string]string) error {
	version := ""
	for key, value := range params {
		switch strings.ToLower(key) {
		case acceptParamVersion:
			version = value
		case acceptParamQ:
			continue
		default:
			return status.Errorf(codes.Unimplemented, "SDMX structure JSON response option %q is not implemented yet", key)
		}
	}
	if version != acceptVersion2 {
		return status.Errorf(codes.Unimplemented, "SDMX structure JSON version %q is not implemented yet", version)
	}
	return nil
}
