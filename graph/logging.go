package graph

import "context"

// Logger is a logger.
//
// You can implement this interface and attach it to the context passed to the graph in order to print graph traversal
// information.
type Logger interface {
	Logf(format string, args ...interface{})
}

// AttachLogger attaches a logger to the context.
func AttachLogger(ctx context.Context, logger Logger) context.Context {
	return context.WithValue(ctx, "graph.logger", logger)
}

func logf(ctx context.Context, format string, args ...interface{}) {
	value := ctx.Value("graph.logger")
	if value == nil {
		return
	}

	logger, ok := value.(Logger)
	if !ok {
		return
	}

	logger.Logf(format, args...)
}

// DefaultLogger creates a default logger that simply calls the supplied function.
func DefaultLogger(fn func(format string, args ...interface{})) Logger {
	return &defaultLogger{fn: fn}
}

type defaultLogger struct {
	fn func(format string, args ...interface{})
}

func (d *defaultLogger) Logf(format string, args ...interface{}) {
	d.fn(format, args...)
}
