package status

import (
	"testing"
	"time"
)

func BenchmarkRepositoryTerminalBurstPublication(b *testing.B) {
	now := time.Unix(20_000, 0).UTC()
	repository, err := NewRepositoryWithClock(DefaultLimits(), func() time.Time { return now })
	if err != nil {
		b.Fatal(err)
	}
	terminals := make([]Terminal, MaxTerminalSummaries)
	for index := range terminals {
		terminals[index] = Terminal{
			IDHash: testHash(uint64(index + 1)), State: FlowClosed,
			Reason: ReasonCompleted, FinishedAt: now.Add(time.Duration(index)),
		}
	}
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for _, terminal := range terminals {
			if err := repository.apply(Event{Kind: EventFlowTerminal, Terminal: terminal}, now); err != nil {
				b.Fatal(err)
			}
		}
		repository.publish(now)
	}
}

func BenchmarkRepositoryActiveFlowPublication(b *testing.B) {
	now := time.Unix(20_000, 0).UTC()
	repository, err := NewRepositoryWithClock(DefaultLimits(), func() time.Time { return now })
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 1024; index++ {
		if err := repository.apply(Event{Kind: EventUpsertFlow, Flow: validTestFlow(uint64(index + 1))}, now); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		repository.publish(now)
	}
}
