package audit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/ai-crypto-onramp/exchange-connector/internal/metrics"
	"github.com/segmentio/kafka-go"
)

const AuditTopic = "audit.v1"

var errNoBrokers = errors.New("audit kafka: no brokers provided")

type AsyncSink struct {
	mu       sync.Mutex
	queue    chan Event
	delegate Sink
	closed   bool
	venue    string
	dropped  int64
}

type gRPCClient struct {
	url   string
	venue string
}

func NewgRPCClient(url, venue string) *gRPCClient {
	return &gRPCClient{url: url, venue: venue}
}

func (g *gRPCClient) Emit(event Event) {
	metrics.AuditEventsEmittedTotal.WithLabelValues(g.venue, string(event.Type)).Inc()
}

func NewAsyncSink(delegate Sink, queueSize int, venue string) *AsyncSink {
	s := &AsyncSink{
		queue:    make(chan Event, queueSize),
		delegate: delegate,
		venue:    venue,
	}
	go s.loop()
	return s
}

func (s *AsyncSink) Emit(event Event) {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	select {
	case s.queue <- event:
	default:
		s.mu.Lock()
		s.dropped++
		s.mu.Unlock()
		metrics.AuditDroppedTotal.WithLabelValues(s.venue, string(event.Type)).Inc()
	}
}

func (s *AsyncSink) loop() {
	for ev := range s.queue {
		if s.delegate != nil {
			s.delegate.Emit(ev)
		}
		metrics.AuditEventsEmittedTotal.WithLabelValues(s.venue, string(ev.Type)).Inc()
	}
}

func (s *AsyncSink) Dropped() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropped
}

func (s *AsyncSink) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()
	close(s.queue)
}

type TracePropagator interface {
	Extract(ctx context.Context) string
}

type noopTrace struct{}

func NewNoopTrace() TracePropagator { return &noopTrace{} }

func (n *noopTrace) Extract(ctx context.Context) string { return "" }

func EmitWithTrace(ctx context.Context, sink Sink, event Event, tp TracePropagator) {
	if tp != nil {
		event.TraceID = tp.Extract(ctx)
	}
	if sink != nil {
		sink.Emit(event)
	}
}

type logSink struct{ logger *log.Logger }

func NewLogSink(logger *log.Logger) Sink {
	if logger == nil {
		logger = log.Default()
	}
	return &logSink{logger: logger}
}

func (l *logSink) Emit(event Event) {
	l.logger.Printf("audit: type=%s venue=%s order=%s", event.Type, event.Venue, event.OrderID)
}

// KafkaSink publishes the canonical audit.v1 envelope (see
// .github/contracts/asyncapi/audit/v1/asyncapi.yaml) to the `audit.v1` Kafka topic.
type KafkaSink struct {
	writer   *kafka.Writer
	venue    string
	dropped  int64
	mu       sync.Mutex
}

// NewKafkaSink returns a KafkaSink targeting the `audit.v1` topic on the
// given brokers. `venue` is the source venue name included in the envelope.
func NewKafkaSink(brokers []string, venue string) (*KafkaSink, error) {
	if len(brokers) == 0 {
		return nil, errNoBrokers
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        AuditTopic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
	}
	return &KafkaSink{writer: w, venue: venue}, nil
}

func (s *KafkaSink) Emit(event Event) {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return
	}
	sum := sha256.Sum256(payload)
	payloadHash := "sha256:" + hex.EncodeToString(sum[:])
	id := event.OrderID
	if id == "" {
		id = event.Venue + "-" + event.Timestamp.Format(time.RFC3339Nano)
	}
	envelope := map[string]any{
		"schema_version": "1",
		"id":              id,
		"ts":              event.Timestamp.UTC().Format(time.RFC3339Nano),
		"source_service":  "exchange-connectors",
		"actor_id":        "exchange-connectors",
		"action":          string(event.Type),
		"target_type":     "order",
		"target_id":       event.OrderID,
		"payload_hash":    payloadHash,
		"payload":         json.RawMessage(payload),
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		return
	}
	if err := s.writer.WriteMessages(context.Background(), kafka.Message{Key: []byte(id), Value: body}); err != nil {
		s.mu.Lock()
		s.dropped++
		s.mu.Unlock()
		metrics.AuditDroppedTotal.WithLabelValues(s.venue, string(event.Type)).Inc()
		return
	}
	metrics.AuditEventsEmittedTotal.WithLabelValues(s.venue, string(event.Type)).Inc()
}

func (s *KafkaSink) Close() error {
	if s.writer == nil {
		return nil
	}
	return s.writer.Close()
}

func (s *KafkaSink) Dropped() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropped
}