package book

import (
	"context"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/exchange-connector/internal/venue"
)

func TestBookAggregatorReceive(t *testing.T) {
	conn := venue.NewDummyVenueConnector()
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	agg, err := NewBookAggregator(ctx, conn, []string{"BTCUSDT"})
	if err != nil {
		t.Fatalf("new aggregator: %v", err)
	}
	defer agg.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := agg.Get("BTCUSDT"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no book update received")
		}
		time.Sleep(50 * time.Millisecond)
	}
	top, ok := agg.Get("BTCUSDT")
	if !ok {
		t.Fatalf("expected book")
	}
	if top.Bid <= 0 || top.Ask <= 0 {
		t.Fatalf("invalid top: %+v", top)
	}
	if top.Bid >= top.Ask {
		t.Fatalf("bid >= ask: %v >= %v", top.Bid, top.Ask)
	}
	if top.Pair != "BTCUSDT" {
		t.Fatalf("pair: %s", top.Pair)
	}
}

func TestBookAggregatorMissingPair(t *testing.T) {
	conn := venue.NewDummyVenueConnector()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	agg, err := NewBookAggregator(ctx, conn, []string{"BTCUSDT"})
	if err != nil {
		t.Fatalf("new aggregator: %v", err)
	}
	defer agg.Close()
	_, ok := agg.Get("NONEXISTENT")
	if ok {
		t.Fatalf("expected no book for unknown pair")
	}
}

func TestBookAggregatorClose(t *testing.T) {
	conn := venue.NewDummyVenueConnector()
	ctx := context.Background()
	agg, err := NewBookAggregator(ctx, conn, []string{"BTCUSDT"})
	if err != nil {
		t.Fatalf("new aggregator: %v", err)
	}
	agg.Close()
	time.Sleep(100 * time.Millisecond)
}