package book

import (
	"context"
	"sync"
	"time"

	"github.com/ai-crypto-onramp/gateway-exchange/internal/venue"
)

type TopOfBook struct {
	Pair string
	Bid  float64
	Ask  float64
	TS   time.Time
}

type BookAggregator struct {
	mu     sync.RWMutex
	latest map[string]TopOfBook
	cancel context.CancelFunc
}

func NewBookAggregator(ctx context.Context, conn venue.VenueConnector, pairs []string) (*BookAggregator, error) {
	ch, err := conn.SubscribeBook(ctx, pairs)
	if err != nil {
		return nil, err
	}
	_, cancel := context.WithCancel(ctx)
	a := &BookAggregator{
		latest: make(map[string]TopOfBook),
		cancel: cancel,
	}
	go a.consume(ch)
	return a, nil
}

func (a *BookAggregator) consume(ch <-chan venue.BookUpdate) {
	for upd := range ch {
		top := TopOfBook{Pair: upd.Pair, TS: upd.TS}
		if len(upd.Bids) > 0 {
			f, _ := upd.Bids[0].Price.Float64()
			top.Bid = f
		}
		if len(upd.Asks) > 0 {
			f, _ := upd.Asks[0].Price.Float64()
			top.Ask = f
		}
		a.mu.Lock()
		a.latest[upd.Pair] = top
		a.mu.Unlock()
	}
}

func (a *BookAggregator) Get(pair string) (TopOfBook, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	t, ok := a.latest[pair]
	return t, ok
}

func (a *BookAggregator) Close() {
	a.cancel()
}