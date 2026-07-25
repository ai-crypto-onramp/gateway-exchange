package ws

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/ai-crypto-onramp/exchange-connector/internal/metrics"
	"github.com/gorilla/websocket"
)

type MessageHandler func(msg []byte)

type Client struct {
	url     string
	venue   string
	handler MessageHandler

	mu       sync.Mutex
	conn     *websocket.Conn
	closed   bool
	maxBackoff time.Duration
}

func defaultMaxBackoff() time.Duration {
	return 30 * time.Second
}

func NewClient(url, venue string) *Client {
	return &Client{
		url:       url,
		venue:     venue,
		maxBackoff: defaultMaxBackoff(),
	}
}

func (c *Client) SetHandler(h MessageHandler) {
	c.mu.Lock()
	c.handler = h
	c.mu.Unlock()
}

func (c *Client) Connect(ctx context.Context) error {
	return c.connect(ctx)
}

func (c *Client) connect(ctx context.Context) error {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}
	conn, _, err := dialer.DialContext(ctx, c.url, http.Header{})
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	return nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	c.closed = true
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (c *Client) Run(ctx context.Context) error {
	backoff := 100 * time.Millisecond
	for {
		c.mu.Lock()
		closed := c.closed
		c.mu.Unlock()
		if closed {
			return nil
		}
		if err := c.connect(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			backoff = nextBackoff(backoff, c.maxBackoff)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			continue
		}
		metrics.WSReconnectCount.WithLabelValues(c.venue).Inc()
		backoff = 100 * time.Millisecond
		err := c.readLoop(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil && !errors.Is(err, io.EOF) {
			_ = err
		}
		c.mu.Lock()
		if c.conn != nil {
			_ = c.conn.Close()
			c.conn = nil
		}
		c.mu.Unlock()
		backoff = nextBackoff(backoff, c.maxBackoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
}

func (c *Client) readLoop(ctx context.Context) error {
	for {
		c.mu.Lock()
		conn := c.conn
		closed := c.closed
		h := c.handler
		c.mu.Unlock()
		if conn == nil || closed {
			return nil
		}
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		if h != nil {
			h(msg)
		}
		_ = ctx
	}
}

func (c *Client) WriteJSON(v interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return errors.New("ws: not connected")
	}
	return c.conn.WriteJSON(v)
}

func nextBackoff(prev, max time.Duration) time.Duration {
	next := time.Duration(float64(prev) * 2)
	jitter := time.Duration(rand.Int63n(int64(prev) + 1))
	next += jitter
	if next > max {
		next = max
	}
	return next
}

type BookReconstructor struct {
	mu          sync.Mutex
	venue       string
	pair        string
	bids        map[string]string
	asks        map[string]string
	lastSeq     int64
	lastUpdate  time.Time
	snapshotTTL time.Duration
}

func NewBookReconstructor(venue, pair string) *BookReconstructor {
	return &BookReconstructor{
		venue:       venue,
		pair:        pair,
		bids:        make(map[string]string),
		asks:        make(map[string]string),
		snapshotTTL: 1 * time.Second,
	}
}

func (r *BookReconstructor) ApplySnapshot(bids, asks [][2]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bids = make(map[string]string)
	r.asks = make(map[string]string)
	for _, lvl := range bids {
		r.bids[lvl[0]] = lvl[1]
	}
	for _, lvl := range asks {
		r.asks[lvl[0]] = lvl[1]
	}
	r.lastUpdate = time.Now()
}

func (r *BookReconstructor) ApplyDiff(seq int64, bids, asks [][2]string) (bool, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastSeq != 0 && seq <= r.lastSeq {
		return false, false
	}
	if r.lastSeq != 0 && seq > r.lastSeq+1 {
		return false, true
	}
	for _, lvl := range bids {
		if lvl[1] == "0" || lvl[1] == "" {
			delete(r.bids, lvl[0])
		} else {
			r.bids[lvl[0]] = lvl[1]
		}
	}
	for _, lvl := range asks {
		if lvl[1] == "0" || lvl[1] == "" {
			delete(r.asks, lvl[0])
		} else {
			r.asks[lvl[0]] = lvl[1]
		}
	}
	r.lastSeq = seq
	r.lastUpdate = time.Now()
	return true, false
}

func (r *BookReconstructor) TopN(n int) (bids, asks [][2]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	bids = collectLevels(r.bids, true, n)
	asks = collectLevels(r.asks, false, n)
	return
}

func (r *BookReconstructor) MarkGap() {
	metrics.WSGapCount.WithLabelValues(r.venue).Inc()
	r.mu.Lock()
	r.lastSeq = 0
	r.mu.Unlock()
}

func (r *BookReconstructor) Lag() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastUpdate.IsZero() {
		return 0
	}
	return time.Since(r.lastUpdate).Seconds()
}

func collectLevels(m map[string]string, descending bool, n int) [][2]string {
	out := make([][2]string, 0, len(m))
	for k, v := range m {
		out = append(out, [2]string{k, v})
	}
	sortLevels(out, descending)
	if len(out) > n {
		out = out[:n]
	}
	return out
}

func sortLevels(s [][2]string, descending bool) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0; j-- {
			a, _ := s[j-1][0], s[j-1][0]
			b, _ := s[j][0], s[j][0]
			less := a < b
			if descending {
				less = a > b
			}
			if less {
				break
			}
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

func ParseMessage(b []byte) (map[string]interface{}, error) {
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}