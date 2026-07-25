package store

import (
	"sync"
	"time"

	"github.com/ai-crypto-onramp/exchange-connector/internal/venue"
	"github.com/shopspring/decimal"
)

type Order struct {
	VenueOrderID  string
	ClientOrderID string
	Venue         string
	Pair          string
	Side          string
	OrderType     string
	Status        string
	FilledQty     decimal.Decimal
	AvgPrice      decimal.Decimal
	Quantity      decimal.Decimal
	Price         decimal.Decimal
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type Fill struct {
	VenueOrderID string
	TradeID      string
	Venue        string
	Pair         string
	Price        decimal.Decimal
	Quantity     decimal.Decimal
	Fee          decimal.Decimal
	FeeAsset     string
	Timestamp    time.Time
}

type Store interface {
	RecordOrder(o Order) (bool, error)
	GetOrderByClientID(clientOrderID string) (*Order, bool)
	RecordFill(f Fill) (bool, error)
	FillsForOrder(venueOrderID string) ([]Fill, error)
}

type MemStore struct {
	mu     sync.RWMutex
	orders map[string]*Order
	fills  []Fill
}

func New() *MemStore {
	return &MemStore{orders: make(map[string]*Order)}
}

func (s *MemStore) RecordOrder(o Order) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.orders[o.ClientOrderID]; ok {
		return false, nil
	}
	if o.CreatedAt.IsZero() {
		o.CreatedAt = time.Now().UTC()
	}
	o.UpdatedAt = time.Now().UTC()
	s.orders[o.ClientOrderID] = &o
	return true, nil
}

func (s *MemStore) GetOrderByClientID(clientOrderID string) (*Order, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	o, ok := s.orders[clientOrderID]
	if !ok {
		return nil, false
	}
	cp := *o
	return &cp, true
}

func (s *MemStore) RecordFill(f Fill) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.fills {
		if existing.Venue == f.Venue && existing.TradeID == f.TradeID {
			return false, nil
		}
	}
	if f.Timestamp.IsZero() {
		f.Timestamp = time.Now().UTC()
	}
	s.fills = append(s.fills, f)
	return true, nil
}

func (s *MemStore) FillsForOrder(venueOrderID string) ([]Fill, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Fill
	for _, f := range s.fills {
		if f.VenueOrderID == venueOrderID {
			out = append(out, f)
		}
	}
	return out, nil
}

var _ = venue.Fill{}