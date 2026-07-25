package events

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/exchange-connector/internal/venue"
	"github.com/shopspring/decimal"
)

type fakePublisher struct {
	mu       sync.Mutex
	fills    [][]byte
	balances [][]byte
	failN    int
	calls    int
}

func (f *fakePublisher) Publish(ctx context.Context, subject string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failN > 0 && f.calls <= f.failN {
		return errors.New("transient")
	}
	if subject == "recon.fills" {
		f.fills = append(f.fills, data)
	} else if subject == "recon.balances" {
		f.balances = append(f.balances, data)
	}
	return nil
}

func (f *fakePublisher) FillCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.fills)
}

func TestPublishFill(t *testing.T) {
	f := &fakePublisher{}
	b := NewBus(f, "recon")
	ev := FillEvent{
		VenueOrderID: "O1",
		Venue:        "binance",
		Pair:         "BTCUSDT",
		Price:        decimal.NewFromInt(50000),
		Size:         decimal.NewFromFloat(0.5),
		Timestamp:    time.Now(),
	}
	if err := b.PublishFill(context.Background(), ev); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if f.FillCount() != 1 {
		t.Fatalf("fills: %d", f.FillCount())
	}
	var got FillEvent
	_ = json.Unmarshal(f.fills[0], &got)
	if got.VenueOrderID != "O1" {
		t.Fatalf("order: %s", got.VenueOrderID)
	}
	if got.InternalID != "O1" {
		t.Fatalf("internal: %s", got.InternalID)
	}
	if !got.Price.Equal(decimal.NewFromInt(50000)) {
		t.Fatalf("price: %v", got.Price)
	}
}

func TestPublishFillRetry(t *testing.T) {
	f := &fakePublisher{failN: 2}
	b := NewBus(f, "recon")
	ev := FillEvent{VenueOrderID: "O1", Venue: "binance", Pair: "BTCUSDT", Timestamp: time.Now()}
	b.maxRetries = 5
	if err := b.PublishFill(context.Background(), ev); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if f.FillCount() != 1 {
		t.Fatalf("fills: %d", f.FillCount())
	}
}

func TestPublishFillDeadLetter(t *testing.T) {
	f := &fakePublisher{failN: 100}
	b := NewBus(f, "recon")
	b.maxRetries = 1
	var dlq FillEvent
	b.SetDeadLetterHandler(func(ev FillEvent, err error) {
		dlq = ev
	})
	err := b.PublishFill(context.Background(), FillEvent{VenueOrderID: "O1", Venue: "binance", Timestamp: time.Now()})
	if err == nil {
		t.Fatalf("expected error")
	}
	if dlq.VenueOrderID != "O1" {
		t.Fatalf("dlq: %+v", dlq)
	}
}

func TestPublishFillMissingOrderID(t *testing.T) {
	f := &fakePublisher{}
	b := NewBus(f, "recon")
	err := b.PublishFill(context.Background(), FillEvent{Venue: "binance"})
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestPublishBalance(t *testing.T) {
	f := &fakePublisher{}
	b := NewBus(f, "recon")
	ev := BalanceEvent{
		Venue:    "binance",
		Asset:    "BTC",
		Free:     decimal.NewFromInt(1),
		Locked:   decimal.NewFromFloat(0.1),
		Timestamp: time.Now(),
	}
	if err := b.PublishBalance(context.Background(), ev); err != nil {
		t.Fatalf("publish: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.balances) != 1 {
		t.Fatalf("balances: %d", len(f.balances))
	}
}

func TestPublishBalanceMissing(t *testing.T) {
	f := &fakePublisher{}
	b := NewBus(f, "recon")
	if err := b.PublishBalance(context.Background(), BalanceEvent{Asset: "BTC"}); err == nil {
		t.Fatalf("expected error")
	}
}

func TestFillFromVenue(t *testing.T) {
	f := venue.Fill{
		VenueOrderID: "O1",
		TradeID:      "T1",
		Price:        decimal.NewFromInt(100),
		Quantity:     decimal.NewFromInt(1),
		Fee:          decimal.NewFromFloat(0.1),
		FeeAsset:     "USDT",
		Timestamp:    time.Now(),
	}
	ev := FillFromVenue("binance", "INT1", "BTCUSDT", f)
	if ev.VenueOrderID != "O1" || ev.InternalID != "INT1" || ev.Venue != "binance" {
		t.Fatalf("ev: %+v", ev)
	}
	if ev.Pair != "BTCUSDT" {
		t.Fatalf("pair: %s", ev.Pair)
	}
}

func TestBalanceFromVenue(t *testing.T) {
	b := BalanceFromVenue("binance", venue.Balance{Asset: "BTC", Free: decimal.NewFromInt(1), Locked: decimal.Zero})
	if b.Venue != "binance" || b.Asset != "BTC" {
		t.Fatalf("b: %+v", b)
	}
}

func TestKafkaPublisherNilWriter(t *testing.T) {
	var p *KafkaPublisher
	if err := p.Publish(context.Background(), "subj", []byte("x")); err == nil {
		t.Fatalf("expected error on nil publisher")
	}
	p = &KafkaPublisher{}
	if err := p.Publish(context.Background(), "subj", []byte("x")); err == nil {
		t.Fatalf("expected error on nil writer")
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close should be no-op on nil writer: %v", err)
	}
}

func TestNewKafkaPublisherNoBrokers(t *testing.T) {
	if _, err := NewKafkaPublisher(nil, ""); err == nil {
		t.Fatalf("expected error for empty brokers")
	}
}

func TestNewKafkaPublisherFromURLBadScheme(t *testing.T) {
	if _, err := NewKafkaPublisherFromURL("nats://x:4222"); err == nil {
		t.Fatalf("expected error for non-kafka scheme")
	}
}

func TestNewKafkaPublisherDefaultTopic(t *testing.T) {
	p, err := NewKafkaPublisher([]string{"broker1:9092"}, "")
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	if p.topic != "recon" {
		t.Fatalf("default topic: %s", p.topic)
	}
	_ = p.Close()
}

func TestNewKafkaPublisherFromURLWithTopic(t *testing.T) {
	cases := []struct {
		url       string
		wantTopic string
	}{
		{"kafka://broker1:9092", "recon"},
		{"kafka://broker1:9092?topic=fills", "fills"},
		{"kafka://broker1:9092,broker2:9092?topic=balances", "balances"},
		{"kafka://broker1:9092,broker2:9092", "recon"},
		{"kafka:// broker1:9092 , broker2:9092 ?topic=settle", "settle"},
	}
	for i, c := range cases {
		p, err := NewKafkaPublisherFromURL(c.url)
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		if p.topic != c.wantTopic {
			t.Fatalf("case %d: topic %s want %s", i, p.topic, c.wantTopic)
		}
		_ = p.Close()
	}
}

func TestNewKafkaPublisherFromURLEmptyBrokers(t *testing.T) {
	if _, err := NewKafkaPublisherFromURL("kafka://"); err == nil {
		t.Fatalf("expected error for empty brokers")
	}
	if _, err := NewKafkaPublisherFromURL("kafka://  "); err == nil {
		t.Fatalf("expected error for whitespace-only brokers")
	}
}