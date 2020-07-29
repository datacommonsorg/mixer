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

package sparql

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

var (
	errBadString = errors.New("bad string")
	errBadEscape = errors.New("bad escape")
)

// isWhitespace returns true if the rune is a space, tab, or newline.
func isWhitespace(ch rune) bool { return ch == ' ' || ch == '\t' || ch == '\n' }

// isLetter returns true if the rune is a letter.
func isLetter(ch rune) bool { return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') }

// isDigit returns true if the rune is a digit.
func isDigit(ch rune) bool { return (ch >= '0' && ch <= '9') }

// isIdentChar returns true if the rune can be used in an unquoted identifier.
func isIdentChar(ch rune) bool {
	return isLetter(ch) || isDigit(ch) || ch == '_' || ch == ':' || ch == '/'
}

// ScanDelimited reads a delimited set of runes.
func ScanDelimited(
	r io.RuneScanner, start, end rune, escapes map[rune]rune, escapesPassThru bool) ([]byte, error) {
	// Scan start delimiter.
	if ch, _, err := r.ReadRune(); err != nil {
		return nil, err
	} else if ch != start {
		return nil, fmt.Errorf("expected %s; found %s", string(start), string(ch))
	}

	var buf bytes.Buffer
	for {
		ch0, _, err := r.ReadRune()
		if ch0 == end {
			return buf.Bytes(), nil
		} else if err != nil {
			return buf.Bytes(), err
		} else if ch0 == '\n' {
			return nil, errors.New("delimited text contains new line")
		} else if ch0 == '\\' {
			// If the next character is an escape then write the escaped char.
			// If it's not a valid escape then return an error.
			ch1, _, err := r.ReadRune()
			if err != nil {
				return nil, err
			}

			c, ok := escapes[ch1]
			if !ok {
				if escapesPassThru {
					// Unread ch1 (char after the \)
					err := r.UnreadRune()
					if err != nil {
						return nil, err
					}
					// Write ch0 (\) to the output buffer.
					buf.WriteRune(ch0)
					continue
				} else {
					buf.Reset()
					buf.WriteRune(ch0)
					buf.WriteRune(ch1)
					return buf.Bytes(), errBadEscape
				}
			}

			buf.WriteRune(c)
		} else {
			buf.WriteRune(ch0)
		}
	}
}

// ScanString reads a quoted string from a rune reader.
func ScanString(r io.RuneScanner) (string, error) {
	ending, _, err := r.ReadRune()
	if err != nil {
		return "", errBadString
	}

	var buf bytes.Buffer
	for {
		ch0, _, err := r.ReadRune()
		if ch0 == ending {
			return buf.String(), nil
		} else if err != nil || ch0 == '\n' {
			return buf.String(), errBadString
		} else if ch0 == '\\' {
			// If the next character is an escape then write the escaped char.
			// If it's not a valid escape then return an error.
			ch1, _, _ := r.ReadRune()
			if ch1 == 'n' {
				buf.WriteRune('\n')
			} else if ch1 == '\\' {
				buf.WriteRune('\\')
			} else if ch1 == '"' {
				buf.WriteRune('"')
			} else if ch1 == '\'' {
				buf.WriteRune('\'')
			} else {
				return string(ch0) + string(ch1), errBadEscape
			}
		} else {
			buf.WriteRune(ch0)
		}
	}
}

// ScanBareIdent reads bare identifier from a rune reader.
func ScanBareIdent(r io.RuneScanner) string {
	// Read every ident character into the buffer.
	// Non-ident characters and EOF will cause the loop to exit.
	var buf bytes.Buffer
	for {
		ch, _, err := r.ReadRune()
		if err != nil {
			break
		} else if !isIdentChar(ch) {
			err := r.UnreadRune()
			if err != nil {
				break
			}
			break
		} else {
			buf.WriteRune(ch)
		}
	}
	return buf.String()
}
