package ratelimit

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ai-crypto-onramp/gateway-exchange/internal/metrics"
)

var ErrBudgetExhausted = errors.New("ratelimit: weight budget exhausted")

type WeightedLimiter struct {
	mu       sync.Mutex
	venue    string
	budget   int64
	used     int64
	refill   time.Duration
	lastFill time.Time
	backoff  time.Time
}

func NewWeightedLimiter(venue string, budget int64) *WeightedLimiter {
	return &WeightedLimiter{
		venue:    venue,
		budget:   budget,
		refill:   60 * time.Second,
		lastFill: time.Now(),
	}
}

func (l *WeightedLimiter) Wait(ctx context.Context, weight int64) error {
	l.mu.Lock()
	l.replenishLocked()
	if !l.backoff.IsZero() && time.Now().Before(l.backoff) {
		l.mu.Unlock()
		return ErrBudgetExhausted
	}
	if l.used+weight > l.budget {
		l.mu.Unlock()
		metrics.RateLimitHeadroom.WithLabelValues(l.venue).Set(0)
		return ErrBudgetExhausted
	}
	l.used += weight
	headroom := float64(l.budget-l.used) / float64(l.budget)
	l.mu.Unlock()
	metrics.RateLimitHeadroom.WithLabelValues(l.venue).Set(headroom)
	return nil
}

func (l *WeightedLimiter) UpdateUsed(venue string, used int64) {
	l.mu.Lock()
	l.venue = venue
	l.used = used
	headroom := float64(l.budget - used)
	if l.budget > 0 {
		headroom = headroom / float64(l.budget)
	}
	l.mu.Unlock()
	metrics.RateLimitHeadroom.WithLabelValues(venue).Set(headroom)
}

func (l *WeightedLimiter) Backoff(venue string, d time.Duration) {
	l.mu.Lock()
	l.venue = venue
	l.backoff = time.Now().Add(d)
	l.mu.Unlock()
}

func (l *WeightedLimiter) Headroom() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.budget == 0 {
		return 0
	}
	return float64(l.budget-l.used) / float64(l.budget)
}

func (l *WeightedLimiter) Used() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.used
}

func (l *WeightedLimiter) Budget() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.budget
}

func (l *WeightedLimiter) SetBudget(b int64) {
	l.mu.Lock()
	l.budget = b
	l.mu.Unlock()
}

func (l *WeightedLimiter) replenishLocked() {
	now := time.Now()
	if now.Sub(l.lastFill) >= l.refill {
		l.used = 0
		l.lastFill = now
	}
}

type CounterLimiter struct {
	mu      sync.Mutex
	venue   string
	budget  int64
	current int64
	last    time.Time
	decay   time.Duration
	backoff time.Time
}

func NewCounterLimiter(venue string, budget int64) *CounterLimiter {
	return &CounterLimiter{
		venue:  venue,
		budget: budget,
		decay:  3 * time.Second,
		last:   time.Now(),
	}
}

func (l *CounterLimiter) Wait(ctx context.Context, cost int64) error {
	l.mu.Lock()
	l.decayLocked()
	if !l.backoff.IsZero() && time.Now().Before(l.backoff) {
		l.mu.Unlock()
		return ErrBudgetExhausted
	}
	if l.current+cost > l.budget {
		l.mu.Unlock()
		metrics.RateLimitHeadroom.WithLabelValues(l.venue).Set(0)
		return ErrBudgetExhausted
	}
	l.current += cost
	headroom := float64(l.budget-l.current) / float64(l.budget)
	l.mu.Unlock()
	metrics.RateLimitHeadroom.WithLabelValues(l.venue).Set(headroom)
	return nil
}

func (l *CounterLimiter) decayLocked() {
	now := time.Now()
	elapsed := now.Sub(l.last)
	ticks := int64(elapsed / l.decay)
	if ticks > 0 {
		l.current -= ticks
		if l.current < 0 {
			l.current = 0
		}
		l.last = l.last.Add(time.Duration(ticks) * l.decay)
	}
}

func (l *CounterLimiter) Backoff(venue string, d time.Duration) {
	l.mu.Lock()
	l.venue = venue
	l.backoff = time.Now().Add(d)
	l.mu.Unlock()
}

func (l *CounterLimiter) Headroom() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.budget == 0 {
		return 0
	}
	return float64(l.budget-l.current) / float64(l.budget)
}

func (l *CounterLimiter) Current() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.current
}

type RPSLimiter struct {
	mu      sync.Mutex
	venue   string
	limit   int64
	count   int64
	window  time.Duration
	start   time.Time
	backoff time.Time
}

func NewRPSLimiter(venue string, rps int64) *RPSLimiter {
	return &RPSLimiter{
		venue:  venue,
		limit:  rps,
		window: time.Second,
		start:  time.Now(),
	}
}

func (l *RPSLimiter) Wait(ctx context.Context, _ int64) error {
	l.mu.Lock()
	now := time.Now()
	if !l.backoff.IsZero() && now.Before(l.backoff) {
		l.mu.Unlock()
		return ErrBudgetExhausted
	}
	if now.Sub(l.start) >= l.window {
		l.count = 0
		l.start = now
	}
	if l.count >= l.limit {
		l.mu.Unlock()
		return ErrBudgetExhausted
	}
	l.count++
	headroom := float64(l.limit-l.count) / float64(l.limit)
	l.mu.Unlock()
	metrics.RateLimitHeadroom.WithLabelValues(l.venue).Set(headroom)
	return nil
}

func (l *RPSLimiter) Backoff(venue string, d time.Duration) {
	l.mu.Lock()
	l.venue = venue
	l.backoff = time.Now().Add(d)
	l.mu.Unlock()
}

func (l *RPSLimiter) Headroom() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.limit == 0 {
		return 0
	}
	return float64(l.limit-l.count) / float64(l.limit)
}