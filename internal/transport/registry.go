package transport

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
)

const MaxRegisteredNameLength = 32

var (
	ErrInvalidName = errors.New("transport: invalid registered name")
	ErrNilFactory  = errors.New("transport: nil factory")
	ErrDuplicate   = errors.New("transport: factory already registered")
	ErrUnknown     = errors.New("transport: factory is not registered")
	ErrNilRegistry = errors.New("transport: nil registry")
)

// ValidName accepts a 1..32 byte lowercase ASCII name. A hyphen may only separate two non-empty segments.
func ValidName(name string) bool {
	if len(name) == 0 || len(name) > MaxRegisteredNameLength {
		return false
	}
	for index := 0; index < len(name); index++ {
		character := name[index]
		if character >= 'a' && character <= 'z' {
			continue
		}
		if index > 0 && character >= '0' && character <= '9' {
			continue
		}
		if character == '-' && index > 0 && index+1 < len(name) && name[index-1] != '-' {
			continue
		}
		return false
	}
	return true
}

// Registry stores carrier factories explicitly enabled in this process. It does
// not register placeholder adapters automatically; startup assembly must
// explicitly register the v1 TCP factory under TCPName.
type Registry struct {
	mu        sync.RWMutex
	factories map[string]Factory
}

func NewRegistry() *Registry {
	return &Registry{factories: make(map[string]Factory)}
}

// Register atomically registers one factory. A failed registration does not replace an existing entry.
func (registry *Registry) Register(name string, factory Factory) error {
	if registry == nil {
		return ErrNilRegistry
	}
	if !ValidName(name) {
		return ErrInvalidName
	}
	if nilInterface(factory) {
		return ErrNilFactory
	}
	if capabilities := factory.Capabilities(); !capabilities.valid() {
		return fmt.Errorf("%w: factory returned zero maximum encoded frame", ErrInvalidCapabilities)
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.factories[name]; exists {
		return ErrDuplicate
	}
	registry.factories[name] = factory
	return nil
}

// Lookup returns a registered factory. Invalid names and valid but unregistered names return distinct errors.
func (registry *Registry) Lookup(name string) (Factory, error) {
	if registry == nil {
		return nil, ErrNilRegistry
	}
	if !ValidName(name) {
		return nil, ErrInvalidName
	}

	registry.mu.RLock()
	factory, exists := registry.factories[name]
	registry.mu.RUnlock()
	if !exists {
		return nil, ErrUnknown
	}
	return factory, nil
}

// Names returns an independent snapshot sorted in ascending byte order.
func (registry *Registry) Names() []string {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	names := make([]string, 0, len(registry.factories))
	for name := range registry.factories {
		names = append(names, name)
	}
	registry.mu.RUnlock()
	sort.Strings(names)
	return names
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
