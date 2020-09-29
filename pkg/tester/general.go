package tester

import (
	"context"
	"time"

	"github.com/coinbase/rosetta-cli/pkg/logger"
)

const (
	// MemoryLoggingFrequency is the frequency that memory
	// usage stats are logged to the terminal.
	MemoryLoggingFrequency = 10 * time.Second
)

// LogMemoryLoop runs a loop that logs memory usage.
func LogMemoryLoop(
	ctx context.Context,
) error {
	ticker := time.NewTicker(MemoryLoggingFrequency)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.LogMemoryStats(ctx)
			return ctx.Err()
		case <-ticker.C:
			logger.LogMemoryStats(ctx)
		}
	}
}
