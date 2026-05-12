package provider

import (
	"errors"
	"log"
	"math/rand"
	"time"
)

// SimulatedProvider pretends to send emails.
// It adds realistic latency and fails ~30% of the time to exercise retry logic.
type SimulatedProvider struct{}

func NewSimulatedProvider() *SimulatedProvider {
	return &SimulatedProvider{}
}

func (s *SimulatedProvider) Send(to, subject, body string) error {
	// Simulate network latency (100–500 ms)
	time.Sleep(time.Duration(100+rand.Intn(400)) * time.Millisecond)

	// Fail ~30% of calls so callers can test exponential backoff
	if rand.Float32() < 0.3 {
		log.Printf("[SimulatedProvider] FAIL sending to %s", to)
		return errors.New("simulated provider: transient network error")
	}

	log.Printf("[SimulatedProvider] OK  email → %s | subject: %s", to, subject)
	return nil
}
