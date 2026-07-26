package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ai-crypto-onramp/gateway-exchange/internal/audit"
	"github.com/ai-crypto-onramp/gateway-exchange/internal/authtoken"
	"github.com/ai-crypto-onramp/gateway-exchange/internal/book"
	"github.com/ai-crypto-onramp/gateway-exchange/internal/events"
	"github.com/ai-crypto-onramp/gateway-exchange/internal/store"
	"github.com/ai-crypto-onramp/gateway-exchange/internal/venue"
	"github.com/shopspring/decimal"
)

type Service struct {
	connector venue.VenueConnector
	book      *book.BookAggregator
	audit     audit.Sink
	events    *events.Bus
	venueName string
	ready     bool
	store     store.Store
}

type Config struct {
	VenueName string
	Pairs     []string
	Store     store.Store
}

func NewService(conn venue.VenueConnector, sink audit.Sink, cfg Config, bus *events.Bus) (*Service, error) {
	ctx, cancel := context.WithCancel(context.Background())
	_ = cancel
	agg, err := book.NewBookAggregator(ctx, conn, cfg.Pairs)
	if err != nil {
		return nil, err
	}
	st := cfg.Store
	if st == nil {
		st = store.New()
	}
	return &Service{
		connector: conn,
		book:     agg,
		audit:    sink,
		events:   bus,
		venueName: cfg.VenueName,
		ready:    true,
		store:    st,
	}, nil
}

func (s *Service) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/admin/status", s.handleAdminStatus)
	mux.HandleFunc("/admin/rotate-credentials", s.handleRotateCreds)
	mux.HandleFunc("/v1/orders", s.handleOrdersRoot)
	mux.HandleFunc("/v1/orders/", s.handleOrderSub)
	mux.HandleFunc("/v1/balances", s.handleBalances)
	mux.HandleFunc("/v1/book/", s.handleBook)
	secret, bypass := authtoken.SecretFromEnv()
	return authtoken.Middleware(secret, bypass)(mux)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Service) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Service) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.ready {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
		return
	}
	writeError(w, http.StatusServiceUnavailable, "not ready")
}

type adminStatus struct {
	Venue             string          `json:"venue"`
	CircuitBreaker    string          `json:"circuit_breaker"`
	RateLimitHeadroom string          `json:"rate_limit_headroom"`
	LastFill          *venue.Fill     `json:"last_fill"`
	Balances          *venue.Balances `json:"balances,omitempty"`
}

func (s *Service) handleAdminStatus(w http.ResponseWriter, r *http.Request) {
	var lastFill *venue.Fill
	if d, ok := s.connector.(*venue.DummyVenueConnector); ok {
		lastFill = d.LastFill()
	}
	resp := adminStatus{
		Venue:             s.venueName,
		CircuitBreaker:    "closed",
		RateLimitHeadroom: "unlimited",
		LastFill:          lastFill,
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Service) handleRotateCreds(w http.ResponseWriter, r *http.Request) {
	if s.audit != nil {
		s.audit.Emit(audit.Event{
			Type:  audit.EventCredRotation,
			Venue: s.venueName,
		})
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "rotated"})
}

type orderRequest struct {
	Venue  string `json:"venue"`
	Pair   string `json:"pair"`
	Side   string `json:"side"`
	Type   string `json:"type"`
	Amount string `json:"amount"`
	Price  string `json:"price"`
}

type orderResponse struct {
	VenueOrderID   string          `json:"venue_order_id"`
	ClientOrderID  string          `json:"client_order_id"`
	Status         string          `json:"status"`
	FilledQty      decimal.Decimal `json:"filled_qty"`
	AvgPrice       decimal.Decimal `json:"avg_price"`
	Fills          []venue.Fill    `json:"fills"`
	Venue          string          `json:"venue"`
	Pair           string          `json:"pair"`
}

func (s *Service) handleOrdersRoot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req orderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.Venue == "" || req.Pair == "" || req.Side == "" || req.Type == "" || req.Amount == "" {
		writeError(w, http.StatusBadRequest, "missing or invalid fields")
		return
	}
	amount, err := decimal.NewFromString(req.Amount)
	if err != nil || amount.LessThanOrEqual(decimal.Zero) {
		writeError(w, http.StatusBadRequest, "invalid amount")
		return
	}
	var price decimal.Decimal
	if req.Price != "" {
		price, err = decimal.NewFromString(req.Price)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid price")
			return
		}
	}
	vReq := venue.OrderRequest{
		Symbol:   req.Pair,
		Side:     venue.Side(req.Side),
		Type:     venue.OrderType(req.Type),
		Quantity: amount,
		Price:    price,
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	resp, err := s.connector.PlaceOrder(ctx, vReq)
	if err != nil {
		writeError(w, http.StatusBadGateway, "place order: "+err.Error())
		return
	}
	if s.store != nil {
		_, _ = s.store.RecordOrder(store.Order{
			VenueOrderID:  resp.VenueOrderID,
			ClientOrderID: resp.ClientOrderID,
			Venue:         s.venueName,
			Pair:          req.Pair,
			Side:          req.Side,
			OrderType:     req.Type,
			Status:        string(resp.Status),
			FilledQty:     resp.FilledQty,
			AvgPrice:      resp.AvgPrice,
			Quantity:      amount,
			Price:         price,
		})
		for _, f := range resp.Fills {
			fc := f
			_, _ = s.store.RecordFill(store.Fill{
				VenueOrderID: resp.VenueOrderID,
				TradeID:      fc.TradeID,
				Venue:        s.venueName,
				Pair:         req.Pair,
				Price:        fc.Price,
				Quantity:     fc.Quantity,
				Fee:          fc.Fee,
				FeeAsset:     fc.FeeAsset,
				Timestamp:    fc.Timestamp,
			})
		}
	}
	if s.audit != nil {
		s.audit.Emit(audit.Event{
			Type:      audit.EventOrderPlaced,
			Venue:     s.venueName,
			OrderID:   resp.VenueOrderID,
			ClientID:  resp.ClientOrderID,
			Pair:      req.Pair,
			Side:      req.Side,
			OrderType: req.Type,
			Quantity:  amount,
			Price:     price,
		})
		for _, f := range resp.Fills {
			fc := f
			s.audit.Emit(audit.Event{
				Type:    audit.EventFill,
				Venue:   s.venueName,
				OrderID: resp.VenueOrderID,
				Pair:    req.Pair,
				Fill: &audit.FillDetail{
					TradeID:  fc.TradeID,
					Price:    fc.Price,
					Quantity: fc.Quantity,
					Fee:      fc.Fee,
					FeeAsset: fc.FeeAsset,
					TS:       fc.Timestamp,
				},
			})
			if s.events != nil {
				_ = s.events.PublishFill(ctx, events.FillFromVenue(s.venueName, resp.VenueOrderID, req.Pair, fc))
			}
		}
	}
	out := orderResponse{
		VenueOrderID:  resp.VenueOrderID,
		ClientOrderID: resp.ClientOrderID,
		Status:        string(resp.Status),
		FilledQty:     resp.FilledQty,
		AvgPrice:      resp.AvgPrice,
		Fills:         resp.Fills,
		Venue:         s.venueName,
		Pair:          req.Pair,
	}
	writeJSON(w, http.StatusOK, out)
}

func splitOrderPath(p string) (id, sub string) {
	trimmed := strings.TrimPrefix(p, "/v1/orders/")
	trimmed = strings.TrimSuffix(trimmed, "/")
	parts := strings.SplitN(trimmed, "/", 2)
	id = parts[0]
	if len(parts) > 1 {
		sub = parts[1]
	}
	return
}

func (s *Service) handleOrderSub(w http.ResponseWriter, r *http.Request) {
	id, sub := splitOrderPath(r.URL.Path)
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing order id")
		return
	}
	switch sub {
	case "cancel":
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if err := s.connector.CancelOrder(ctx, venue.CancelRequest{VenueOrderID: id}); err != nil {
			writeError(w, http.StatusBadGateway, "cancel: "+err.Error())
			return
		}
		if s.audit != nil {
			s.audit.Emit(audit.Event{
				Type:    audit.EventOrderCanceled,
				Venue:   s.venueName,
				OrderID: id,
			})
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "canceled", "order_id": id})
	case "fills":
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		limit := 0
		if q := r.URL.Query().Get("limit"); q != "" {
			if n, err := strconv.Atoi(q); err == nil {
				limit = n
			}
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		fills, err := s.connector.GetFills(ctx, venue.FillQuery{VenueOrderID: id, Limit: limit})
		if err != nil {
			writeError(w, http.StatusBadGateway, "fills: "+err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"order_id": id, "fills": fills})
	case "status", "":
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		o, err := s.connector.GetOrder(ctx, id)
		if err != nil {
			writeError(w, http.StatusBadGateway, "get order: "+err.Error())
			return
		}
		writeJSON(w, http.StatusOK, o)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (s *Service) handleBalances(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	b, err := s.connector.GetBalances(ctx)
	if err != nil {
		writeError(w, http.StatusBadGateway, "balances: "+err.Error())
		return
	}
	if s.audit != nil {
		for _, bal := range b.Assets {
			bc := bal
			s.audit.Emit(audit.Event{
				Type:    audit.EventBalanceSnapshot,
				Venue:   s.venueName,
				Balance: &audit.BalanceDetail{Asset: bc.Asset, Free: bc.Free, Locked: bc.Locked},
			})
			if s.events != nil {
				_ = s.events.PublishBalance(ctx, events.BalanceFromVenue(s.venueName, bc))
			}
		}
	}
	writeJSON(w, http.StatusOK, b)
}

func (s *Service) handleBook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	pair := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/book/"), "/")
	if pair == "" {
		writeError(w, http.StatusBadRequest, "missing pair")
		return
	}
	top, ok := s.book.Get(pair)
	if !ok {
		writeError(w, http.StatusNotFound, "no book for pair")
		return
	}
	writeJSON(w, http.StatusOK, top)
}