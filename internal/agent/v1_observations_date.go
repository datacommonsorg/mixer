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

package agent

import (
	"fmt"
	"regexp"
	"time"
)

var dateFormatRegex = regexp.MustCompile(`^\d{4}(-\d{2})?(-\d{2})?$`)

type dateFilter struct {
	dateType  string // dateTypeLatest, dateTypeAll, or dateTypeRange
	startDate time.Time
	endDate   time.Time
}

// validateDateFormat checks if a string is a valid ISO-8601 date subset (YYYY, YYYY-MM, YYYY-MM-DD).
func validateDateFormat(date string) error {
	if !dateFormatRegex.MatchString(date) {
		return fmt.Errorf("invalid date format: %q", date)
	}
	// Try parsing it to catch things like "2023-99"
	_, _, err := parseDateStringToInterval(date)
	return err
}

// parseDateStringToInterval converts a partial date string into start and end time boundaries.
// All calculations are timezone-agnostic and default strictly to UTC.
func parseDateStringToInterval(dateStr string) (time.Time, time.Time, error) {
	var start, end time.Time
	var err error

	switch len(dateStr) {
	case 4: // YYYY
		start, err = time.Parse("2006-01-02", dateStr+"-01-01")
		if err != nil {
			return start, end, err
		}
		end = start.AddDate(1, 0, -1) // End of year (December 31st)
	case 7: // YYYY-MM
		start, err = time.Parse("2006-01-02", dateStr+"-01")
		if err != nil {
			return start, end, err
		}
		end = start.AddDate(0, 1, -1) // End of month
	case 10: // YYYY-MM-DD
		start, err = time.Parse("2006-01-02", dateStr)
		if err != nil {
			return start, end, err
		}
		end = start
	default:
		return start, end, fmt.Errorf("unsupported date length: %q", dateStr)
	}
	return start, end, nil
}

// parseDateFilter validates date configurations and returns an internal date filter.
func parseDateFilter(date string, startStr string, endStr string) (*dateFilter, error) {
	if date == "" {
		date = dateTypeLatest
	}

	dateLower := date
	if dateLower == dateTypeLatest || dateLower == dateTypeAll {
		return &dateFilter{dateType: dateLower}, nil
	}

	if dateLower == dateTypeRange {
		if startStr == "" && endStr == "" {
			return nil, fmt.Errorf("must specify start_date or end_date when date is 'range'")
		}
		var startVal, endVal time.Time
		if startStr != "" {
			if err := validateDateFormat(startStr); err != nil {
				return nil, fmt.Errorf("invalid start_date: %w", err)
			}
			s, _, err := parseDateStringToInterval(startStr)
			if err != nil {
				return nil, fmt.Errorf("invalid start_date: %w", err)
			}
			startVal = s
		}
		if endStr != "" {
			if err := validateDateFormat(endStr); err != nil {
				return nil, fmt.Errorf("invalid end_date: %w", err)
			}
			_, e, err := parseDateStringToInterval(endStr)
			if err != nil {
				return nil, fmt.Errorf("invalid end_date: %w", err)
			}
			endVal = e
		}
		if !startVal.IsZero() && !endVal.IsZero() && startVal.After(endVal) {
			return nil, fmt.Errorf("start_date cannot be after end_date")
		}
		return &dateFilter{
			dateType:  dateTypeRange,
			startDate: startVal,
			endDate:   endVal,
		}, nil
	}

	// Specific date string
	if err := validateDateFormat(date); err != nil {
		return nil, err
	}
	s, e, err := parseDateStringToInterval(date)
	if err != nil {
		return nil, err
	}
	return &dateFilter{
		dateType:  dateTypeRange,
		startDate: s,
		endDate:   e,
	}, nil
}

// isDateInInterval checks if an observation date string falls within the filter range.
func isDateInInterval(obsDate string, filter *dateFilter) bool {
	if filter == nil || filter.dateType == dateTypeAll || filter.dateType == dateTypeLatest {
		return true
	}
	// Parse observation date (can be YYYY, YYYY-MM, YYYY-MM-DD)
	t, _, err := parseDateStringToInterval(obsDate)
	if err != nil {
		return false
	}
	// Check range boundaries (inclusive)
	if !filter.startDate.IsZero() && t.Before(filter.startDate) {
		return false
	}
	if !filter.endDate.IsZero() && t.After(filter.endDate) {
		return false
	}
	return true
}
