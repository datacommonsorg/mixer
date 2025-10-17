// Copyright 2025 Google LLC
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

package redis

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	metadataURL = "http://metadata.google.internal/computeMetadata/v1/instance/zone"
)

// RedisConfig represents the overall Redis configuration.
type RedisConfig struct {
	// Ideally, we would have a map of region to RedisInstanceConfig,
	// but we want to default to the first region if the current region is not found
	// and the order of the map is not guaranteed.
	// So using a slice of RedisInstanceConfig instead.
	Instances []RedisInstanceConfig `yaml:"instances"`
}

// RedisInstanceConfig represents the configuration for a specific Redis instance.
type RedisInstanceConfig struct {
	Region string `yaml:"region"`
	Host   string `yaml:"host"`
	Port   string `yaml:"port"`
}

// Address returns the address string ("host:port") for the Redis instance.
func (c RedisInstanceConfig) Address() string {
	return c.Host + ":" + c.Port
}

// getRegion retrieves the region from the metadata server and returns an error if any.
func getRegion() (string, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", metadataURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create metadata request: %w", err)
	}
	req.Header.Set("Metadata-Flavor", "Google")

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to get metadata: %w", err)
	}
	//nolint:errcheck // TODO: Fix pre-existing issue and remove comment.
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata request failed with status: %d", resp.StatusCode)
	}

	buf := new(strings.Builder)
	_, err = io.Copy(buf, resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read metadata response: %w", err)
	}
	zone := buf.String()

	// zone is in the format of projects/projectnum/zones/zone
	parts := strings.Split(zone, "/")
	if len(parts) < 4 {
		return "", fmt.Errorf("invalid zone format: %s", zone)
	}

	regionParts := strings.Split(parts[3], "-")
	if len(regionParts) < 2 {
		return "", fmt.Errorf("invalid zone format: %s", zone)
	}

	return strings.Join(regionParts[:2], "-"), nil
}

// getRegionWithLogging retrieves the region from the metadata server and logs any errors.
func getRegionWithLogging() string {
	region, err := getRegion()
	if err != nil {
		slog.Warn("Failed to get region (the first redis instance in the config will be used)", "error", err)
		return ""
	}
	return region
}

// getRedisAddress retrieves the Redis address string ("host:port") from a YAML string for the specified region.
func getRedisAddress(yamlStr string, region string) (string, error) {
	var config RedisConfig
	err := yaml.Unmarshal([]byte(yamlStr), &config)
	if err != nil {
		return "", fmt.Errorf("failed to decode redis config: %w", err)
	}

	// Find a matching region
	for _, regionConfig := range config.Instances {
		if regionConfig.Region == region {
			return regionConfig.Address(), nil
		}
	}

	// If no region match, choose the first region
	if len(config.Instances) > 0 {
		return config.Instances[0].Address(), nil
	}

	return "", fmt.Errorf("no instances found in redis config")
}

// GetRedisAddress retrieves the Redis address string ("host:port") from a YAML string for the region retrieved from the Google metadata server.
func GetRedisAddress(yamlConfig string) (string, error) {
	region := getRegionWithLogging()
	return getRedisAddress(yamlConfig, region)
}
