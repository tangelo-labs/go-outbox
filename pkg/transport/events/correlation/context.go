package correlation

import (
	"context"
)

type contextKey uint8

const correlationKey = contextKey(157)

// FromContext extracts correlation id from context if available, an empty string will be returned if not available.
func FromContext(ctx context.Context) string {
	if v, ok := ctx.Value(correlationKey).(string); ok {
		return v
	}

	return ""
}

// AddToContext injects correlation id information into the given context.
func AddToContext(ctx context.Context, correlationID string) context.Context {
	return context.WithValue(ctx, correlationKey, correlationID)
}
