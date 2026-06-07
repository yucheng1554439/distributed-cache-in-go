package config

import (
	"os"
	"strconv"
	"strings"
)

// StringFromEnv returns the flag value or falls back to an environment variable.
func StringFromEnv(flagValue, envKey string) string {
	if strings.TrimSpace(flagValue) != "" {
		return flagValue
	}
	return strings.TrimSpace(os.Getenv(envKey))
}

// BoolFromEnv returns the flag value unless the environment variable overrides it.
func BoolFromEnv(flagValue bool, envKey string) bool {
	raw := strings.TrimSpace(os.Getenv(envKey))
	if raw == "" {
		return flagValue
	}
	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		return flagValue
	}
	return parsed
}

// IntFromEnv returns the flag value unless the environment variable overrides a zero value.
func IntFromEnv(flagValue int, envKey string) int {
	raw := strings.TrimSpace(os.Getenv(envKey))
	if raw == "" {
		return flagValue
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return flagValue
	}
	return parsed
}

// Int64FromEnv returns the flag value unless the environment variable overrides a zero value.
func Int64FromEnv(flagValue int64, envKey string) int64 {
	raw := strings.TrimSpace(os.Getenv(envKey))
	if raw == "" {
		return flagValue
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return flagValue
	}
	return parsed
}
