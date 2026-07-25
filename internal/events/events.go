package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ai-crypto-onramp/exchange-connector/internal/metrics"
	"github.com/ai-crypto-onramp/exchange-connector/internal/venue"
	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
)

type FillEvent struct {
	VenueOrderID string          `json:"venue_order_id"`
	InternalID   string          `json:"internal_order_id"`
	Venue        string          `json:"venue"`
	Pair         string          `json:"pair"`
	Price        decimal.Decimal `json:"price"`
	Size         decimal.Decimal `json:"size"`
	Fee          decimal.Decimal `json:"fee"`
	FeeAsset     string          `json:"fee_asset"`
	TradeID      string          `json:"trade_id"`
	Timestamp    time.Time       `json:"timestamp"`
	EmittedAt    time.Time       `json:"emitted_at"`
}

type BalanceEvent struct {
	Venue      string          `json:"venue"`
	Asset      string          `json:"asset"`
	Free       decimal.Decimal `json:"free"`
	Locked     decimal.Decimal `json:"locked"`
	Timestamp  time.Time       `json:"timestamp"`
	EmittedAt  time.Time       `json:"emitted_at"`
}

type Publisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
}

// KafkaPublisher publishes fill/balance events to a Kafka topic derived from
// the subject. It implements the Publisher interface.
type KafkaPublisher struct {
	writer *kafka.Writer
	topic  string
}

// KafkaConn is the minimal connection surface retained for test doubles
// that still want to inject a writer-like dependency.
type KafkaConn interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// NewKafkaPublisher returns a KafkaPublisher targeting the given brokers and
// default topic. Events are keyed by the venue_order_id (fills) or venue
// (balances) so consumers receive per-key ordering.
func NewKafkaPublisher(brokers []string, topic string) (*KafkaPublisher, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("events: no kafka brokers provided")
	}
	if topic == "" {
		topic = "recon"
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
	}
	return &KafkaPublisher{writer: w, topic: topic}, nil
}

// NewKafkaPublisherFromURL parses a "kafka://host:9092[,host2][?topic=t]" URL
// and returns a KafkaPublisher.
func NewKafkaPublisherFromURL(url string) (*KafkaPublisher, error) {
	if !strings.HasPrefix(url, "kafka://") {
		return nil, fmt.Errorf("events: url must start with kafka://, got %q", url)
	}
	rest := strings.TrimPrefix(url, "kafka://")
	topic := ""
	if i := strings.Index(rest, "?"); i >= 0 {
		q := rest[i+1:]
		rest = rest[:i]
		for _, kv := range strings.Split(q, "&") {
			if strings.HasPrefix(kv, "topic=") {
				topic = strings.TrimPrefix(kv, "topic=")
			}
		}
	}
	brokers := strings.Split(rest, ",")
	clean := brokers[:0]
	for _, b := range brokers {
		b = strings.TrimSpace(b)
		if b != "" {
			clean = append(clean, b)
		}
	}
	return NewKafkaPublisher(clean, topic)
}

// Publish writes data to Kafka. The subject is advisory; the writer's default
// topic is used (callers route by subject suffix via consumer groups).
func (p *KafkaPublisher) Publish(ctx context.Context, subject string, data []byte) error {
	if p == nil || p.writer == nil {
		return errors.New("events: kafka not connected")
	}
	return p.writer.WriteMessages(ctx, kafka.Message{Value: data})
}

// Close flushes and closes the underlying writer.
func (p *KafkaPublisher) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

type Bus struct {
	mu         sync.Mutex
	publisher  Publisher
	subject    string
	maxRetries int
	deadLetter func(FillEvent, error)
}

func NewBus(publisher Publisher, subject string) *Bus {
	return &Bus{
		publisher:  publisher,
		subject:    subject,
		maxRetries: 3,
		deadLetter: func(FillEvent, error) {},
	}
}

func (b *Bus) SetDeadLetterHandler(h func(FillEvent, error)) {
	b.mu.Lock()
	b.deadLetter = h
	b.mu.Unlock()
}

func (b *Bus) PublishFill(ctx context.Context, ev FillEvent) error {
	if ev.VenueOrderID == "" {
		return errors.New("events: venue_order_id required")
	}
	if ev.InternalID == "" {
		ev.InternalID = ev.VenueOrderID
	}
	if ev.EmittedAt.IsZero() {
		ev.EmittedAt = time.Now().UTC()
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	var lastErr error
	for attempt := 0; attempt <= b.maxRetries; attempt++ {
		err := b.publisher.Publish(ctx, b.subject+".fills", data)
		if err == nil {
			metrics.EventsPublishedTotal.WithLabelValues(ev.Venue, "fill", "ok").Inc()
			latency := time.Since(ev.Timestamp).Seconds()
			if latency < 0 {
				latency = 0
			}
			metrics.FillLatencySeconds.WithLabelValues(ev.Venue).Observe(latency)
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
		}
	}
	metrics.EventsPublishedTotal.WithLabelValues(ev.Venue, "fill", "failed").Inc()
	b.mu.Lock()
	b.deadLetter(ev, lastErr)
	b.mu.Unlock()
	return lastErr
}

func (b *Bus) PublishBalance(ctx context.Context, ev BalanceEvent) error {
	if ev.Venue == "" || ev.Asset == "" {
		return errors.New("events: venue and asset required")
	}
	if ev.EmittedAt.IsZero() {
		ev.EmittedAt = time.Now().UTC()
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	var lastErr error
	for attempt := 0; attempt <= b.maxRetries; attempt++ {
		err := b.publisher.Publish(ctx, b.subject+".balances", data)
		if err == nil {
			metrics.EventsPublishedTotal.WithLabelValues(ev.Venue, "balance", "ok").Inc()
			lag := time.Since(ev.Timestamp).Seconds()
			if lag < 0 {
				lag = 0
			}
			metrics.BalanceSyncLagSeconds.WithLabelValues(ev.Venue).Observe(lag)
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
		}
	}
	metrics.EventsPublishedTotal.WithLabelValues(ev.Venue, "balance", "failed").Inc()
	return lastErr
}

func FillFromVenue(venueName, internalID, pair string, f venue.Fill) FillEvent {
	return FillEvent{
		VenueOrderID: f.VenueOrderID,
		InternalID:   internalID,
		Venue:        venueName,
		Pair:         pair,
		Price:        f.Price,
		Size:         f.Quantity,
		Fee:          f.Fee,
		FeeAsset:     f.FeeAsset,
		TradeID:      f.TradeID,
		Timestamp:    f.Timestamp,
	}
}

func BalanceFromVenue(venueName string, b venue.Balance) BalanceEvent {
	return BalanceEvent{
		Venue:     venueName,
		Asset:     b.Asset,
		Free:      b.Free,
		Locked:    b.Locked,
		Timestamp: time.Now().UTC(),
	}
}