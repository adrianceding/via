// Package path discovers and filters local network paths without dialing.
package path

import (
	"errors"
	"fmt"
	stdpath "path"
)

const (
	MaxIncludePatterns = 64
	MaxExcludePatterns = 64
	MaxPatternBytes    = 64
)

var ErrInvalidPattern = errors.New("path: invalid interface pattern")

type Filter struct {
	include []string
	exclude []string
}

func NewFilter(include, exclude []string) (Filter, error) {
	if err := validatePatterns(include, MaxIncludePatterns); err != nil {
		return Filter{}, err
	}
	if err := validatePatterns(exclude, MaxExcludePatterns); err != nil {
		return Filter{}, err
	}
	return Filter{
		include: append([]string(nil), include...),
		exclude: append([]string(nil), exclude...),
	}, nil
}

func (filter Filter) Include() []string { return append([]string(nil), filter.include...) }
func (filter Filter) Exclude() []string { return append([]string(nil), filter.exclude...) }

func (filter Filter) Match(interfaceName string) (bool, DecisionReason) {
	if matchesAny(filter.exclude, interfaceName) {
		return false, ReasonExcluded
	}
	if len(filter.include) != 0 && !matchesAny(filter.include, interfaceName) {
		return false, ReasonNotIncluded
	}
	return true, ReasonEligible
}

func validatePatterns(patterns []string, maximum int) error {
	if len(patterns) > maximum {
		return fmt.Errorf("%w: too many patterns", ErrInvalidPattern)
	}
	for _, pattern := range patterns {
		if len(pattern) == 0 || len(pattern) > MaxPatternBytes {
			return fmt.Errorf("%w: pattern length", ErrInvalidPattern)
		}
		if _, err := stdpath.Match(pattern, ""); err != nil {
			return fmt.Errorf("%w: syntax", ErrInvalidPattern)
		}
	}
	return nil
}

func matchesAny(patterns []string, interfaceName string) bool {
	for _, pattern := range patterns {
		matched, _ := stdpath.Match(pattern, interfaceName)
		if matched {
			return true
		}
	}
	return false
}
