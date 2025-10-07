package log

import (
	"log/slog"
	"os"
)

// One-time setup of structured logging. This is used throughout Mixer.
// Structured logs can be automatically ingested by Cloud Logging with their key-value pairs preserved, see https://go.dev/blog/slog.
// The NewJSONHandler specifies that logs should be in JSON format and streamed to stdout.
// We also add the source (function, file, and line number where the log originates) to every log by default.
func SetUpLogger() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{AddSource: true}))
	slog.SetDefault(logger)
}