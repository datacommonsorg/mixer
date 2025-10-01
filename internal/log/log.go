package log

import (
	"log/slog"
	"os"
)

// SetUpLogger sets up a logger.
func SetUpLogger() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{AddSource: true}))
	slog.SetDefault(logger)
}
