package path

import (
	"context"
	"errors"
	"time"
)

const DiscoveryInterval = time.Second

var ErrUpdateQueueFull = errors.New("path: update queue is full")

type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type Clock interface {
	NewTicker(time.Duration) Ticker
}

type SystemClock struct{}

func (SystemClock) NewTicker(interval time.Duration) Ticker {
	return systemTicker{Ticker: time.NewTicker(interval)}
}

type systemTicker struct{ *time.Ticker }

func (ticker systemTicker) C() <-chan time.Time { return ticker.Ticker.C }

type Update struct {
	Events   []Event
	Snapshot Snapshot
	Err      error
}

// Watch performs the initial refresh synchronously and then one refresh per
// injected tick. The caller owns the goroutine and bounded output channel.
func (manager *Manager) Watch(ctx context.Context, clock Clock, output chan<- Update) error {
	if manager == nil || ctx == nil || clock == nil || output == nil || cap(output) < 1 || cap(output) > MaxEventsPerRefresh {
		return ErrEnumeration
	}
	publish := func() error {
		events, snapshot, err := manager.Refresh()
		update := Update{Events: events, Snapshot: snapshot, Err: err}
		select {
		case output <- update:
			return nil
		default:
			return ErrUpdateQueueFull
		}
	}
	if err := publish(); err != nil {
		return err
	}
	ticker := clock.NewTicker(DiscoveryInterval)
	if ticker == nil || ticker.C() == nil {
		return ErrEnumeration
	}
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C():
			if err := publish(); err != nil {
				return err
			}
		}
	}
}
