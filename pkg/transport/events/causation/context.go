package causation

import (
	"context"
)

type contextKey uint8

const causationKey = contextKey(117)

// FromContext extracts causation id from context if available, an empty string will be returned if not available.
func FromContext(ctx context.Context) string {
	if v, ok := ctx.Value(causationKey).(string); ok {
		return v
	}

	return ""
}

// AddToContext injects causation id information into the given context.
func AddToContext(ctx context.Context, causationID string) context.Context {
	return context.WithValue(ctx, causationKey, causationID)
}
