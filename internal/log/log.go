package log

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
)

// One-time setup of structured logging. This is used throughout Mixer.
// Structured logs can be automatically ingested by Cloud Logging with their key-value pairs preserved, see https://go.dev/blog/slog.
// The NewJSONHandler specifies that logs should be in JSON format and streamed to stdout.
// We also add the source (function, file, and line number where the log originates) to every log by default.
func SetUpLogger() {
	// When running locally, we use a custom text handler for cleaner, more readable output.
	if os.Getenv("MIXER_LOCAL_LOGS") == "true" {
		setUpLocalLogger()
	} else {
		logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{AddSource: true}))
		slog.SetDefault(logger)
	}
}

// setUpLocalLogger configures a custom handler for local development that provides
// clean, color-coded, and thread-safe logging.
func setUpLocalLogger() {
	logger := slog.New(NewCustomTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)
}

// CustomTextHandler is a slog.Handler that writes logs in a custom human-readable format.
type CustomTextHandler struct {
	opts         slog.HandlerOptions
	mu           *sync.Mutex
	w            io.Writer
	attrs        []slog.Attr
	groups       []string
	excludedKeys map[string]struct{}
}

// NewCustomTextHandler creates a new CustomTextHandler.
func NewCustomTextHandler(w io.Writer, opts *slog.HandlerOptions) *CustomTextHandler {
	if opts == nil {
		opts = &slog.HandlerOptions{}
	}
	return &CustomTextHandler{
		opts: *opts,
		mu:   new(sync.Mutex),
		w:    w,
		excludedKeys: map[string]struct{}{
			slog.LevelKey:   {},
			slog.MessageKey: {},
			slog.SourceKey:  {},
		},
	}
}

func (h *CustomTextHandler) Enabled(_ context.Context, level slog.Level) bool {
	minLevel := slog.LevelInfo
	if h.opts.Level != nil {
		minLevel = h.opts.Level.Level()
	}
	return level >= minLevel
}

func (h *CustomTextHandler) Handle(_ context.Context, r slog.Record) error {
	buf := make([]byte, 0, 1024)

	// Color based on level and highlight
	highlight := false
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "highlight" {
			highlight = a.Value.Bool()
			return false // Stop iterating once found
		}
		return true
	})
	var color string
	if highlight {
		color = "\033[92m" // Green
	} else {
		switch r.Level {
		case slog.LevelWarn:
			color = "\033[93m" // Yellow
		case slog.LevelError:
			color = "\033[91m" // Red
		}
	}

	// Start color if any.
	if color != "" {
		buf = append(buf, color...)
	}

	// Time
	if !r.Time.IsZero() {
		buf = r.Time.AppendFormat(buf, "15:04:05 ")
	}

	// Message.
	buf = fmt.Appendf(buf, "%s\n", r.Message)

	// Attributes
	r.Attrs(func(a slog.Attr) bool {
		if _, ok := h.excludedKeys[a.Key]; !ok && a.Key != "highlight" {
			buf = fmt.Appendf(buf, "    %s: %s\n", a.Key, a.Value.String())
		}
		return true
	})

	// Groups and attributes from WithGroup and WithAttrs.
	for _, g := range h.groups {
		buf = fmt.Appendf(buf, "    %s:\n", g)
	}
	for _, a := range h.attrs {
		buf = fmt.Appendf(buf, "    %s: %s\n", a.Key, a.Value.String())
	}

	// Reset color if any.
	if color != "" {
		buf = append(buf, "\033[0m"...)
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := h.w.Write(buf)
	return err
}

func (h *CustomTextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newH := *h
	newH.attrs = append(h.attrs, attrs...)
	return &newH
}

func (h *CustomTextHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	newH := *h
	newH.groups = append(h.groups, name)
	return &newH
}
