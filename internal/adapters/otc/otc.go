package otc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ai-crypto-onramp/exchange-connector/internal/ratelimit"
	"github.com/ai-crypto-onramp/exchange-connector/internal/venue"
	"github.com/shopspring/decimal"
)

var ErrUnsupported = errors.New("otc: SubscribeOrderBook unsupported")

type Connector struct {
	cfg      venue.VenueConfig
	apiKey   string
	limiter  *ratelimit.RPSLimiter
	mu       sync.Mutex
	orders   map[string]*venue.OrderResponse
	fills    []venue.Fill
	sftpPath string
}

func NewConnector(apiKey string, limiter *ratelimit.RPSLimiter) *Connector {
	rest := os.Getenv("OTC_DESK_URL")
	if rest == "" {
		rest = "http://localhost:9000"
	}
	rps := int64(10)
	if v := os.Getenv("RATE_LIMIT_WEIGHT_BUDGET"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			rps = n
		}
	}
	if limiter == nil {
		limiter = ratelimit.NewRPSLimiter("otc", rps)
	}
	return &Connector{
		cfg: venue.VenueConfig{
			Name:     "otc",
			RESTBase: rest,
		},
		apiKey:   apiKey,
		limiter:  limiter,
		orders:   make(map[string]*venue.OrderResponse),
		sftpPath: os.Getenv("OTC_SFTP_PATH"),
	}
}

var _ venue.VenueConnector = (*Connector)(nil)

type rfqRequest struct {
	ClientOrderID string          `json:"client_order_id"`
	Symbol        string          `json:"symbol"`
	Side          string          `json:"side"`
	Quantity      decimal.Decimal `json:"quantity"`
}

type quoteResp struct {
	QuoteID    string          `json:"quote_id"`
	Price      decimal.Decimal `json:"price"`
	ExpiresAt  time.Time        `json:"expires_at"`
	Status     string          `json:"status"`
}

type acceptResp struct {
	OrderID    string          `json:"order_id"`
	Status     string          `json:"status"`
	FilledQty  decimal.Decimal `json:"filled_qty"`
	Price      decimal.Decimal `json:"price"`
	Fee        decimal.Decimal `json:"fee"`
	FeeAsset   string          `json:"fee_asset"`
	TradeID    string          `json:"trade_id"`
}

func (c *Connector) PlaceOrder(ctx context.Context, req venue.OrderRequest) (*venue.OrderResponse, error) {
	if req.ClientOrderID == "" {
		return nil, fmt.Errorf("otc: client_order_id required")
	}
	body, err := c.do(ctx, http.MethodPost, "/v1/rfq", rfqRequest{
		ClientOrderID: req.ClientOrderID,
		Symbol:        req.Symbol,
		Side:          string(req.Side),
		Quantity:      req.Quantity,
	})
	if err != nil {
		return nil, err
	}
	var q quoteResp
	if err := json.Unmarshal(body, &q); err != nil {
		return nil, err
	}
	if strings.ToLower(q.Status) == "rejected" || strings.ToLower(q.Status) == "expired" {
		return nil, fmt.Errorf("otc: quote %s", q.Status)
	}
	acceptBody, err := c.do(ctx, http.MethodPost, "/v1/quotes/"+q.QuoteID+"/accept", map[string]string{"client_order_id": req.ClientOrderID})
	if err != nil {
		return nil, err
	}
	var a acceptResp
	if err := json.Unmarshal(acceptBody, &a); err != nil {
		return nil, err
	}
	fill := venue.Fill{
		VenueOrderID: a.OrderID,
		TradeID:      a.TradeID,
		Price:        a.Price,
		Quantity:     a.FilledQty,
		Fee:          a.Fee,
		FeeAsset:     a.FeeAsset,
		Timestamp:    time.Now().UTC(),
	}
	resp := &venue.OrderResponse{
		VenueOrderID:  a.OrderID,
		ClientOrderID: req.ClientOrderID,
		Status:        mapStatus(a.Status),
		FilledQty:     a.FilledQty,
		AvgPrice:      a.Price,
		Fills:         []venue.Fill{fill},
	}
	c.mu.Lock()
	c.orders[a.OrderID] = resp
	c.fills = append(c.fills, fill)
	c.mu.Unlock()
	return resp, nil
}

type orderStatusResp struct {
	OrderID   string          `json:"order_id"`
	Status    string          `json:"status"`
	FilledQty decimal.Decimal `json:"filled_qty"`
	Price     decimal.Decimal `json:"price"`
	SettlementStatus string   `json:"settlement_status"`
}

func (c *Connector) GetOrder(ctx context.Context, venueOrderID string) (*venue.OrderResponse, error) {
	body, err := c.do(ctx, http.MethodGet, "/v1/orders/"+venueOrderID, nil)
	if err != nil {
		return nil, err
	}
	var r orderStatusResp
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, err
	}
	status := mapStatus(r.Status)
	if r.SettlementStatus != "" && strings.ToLower(r.SettlementStatus) == "settled" && status == venue.OrderStatusNew {
		status = venue.OrderStatusFilled
	}
	return &venue.OrderResponse{
		VenueOrderID: r.OrderID,
		Status:       status,
		FilledQty:    r.FilledQty,
		AvgPrice:     r.Price,
	}, nil
}

func (c *Connector) CancelOrder(ctx context.Context, req venue.CancelRequest) error {
	id := req.VenueOrderID
	if id == "" {
		id = req.ClientOrderID
	}
	if id == "" {
		return fmt.Errorf("otc: cancel requires order id")
	}
	_, err := c.do(ctx, http.MethodPost, "/v1/orders/"+id+"/cancel", nil)
	return err
}

type fillsResp struct {
	Fills []struct {
		TradeID string          `json:"trade_id"`
		Price   decimal.Decimal `json:"price"`
		Qty     decimal.Decimal `json:"qty"`
		Fee     decimal.Decimal `json:"fee"`
		FeeAsset string         `json:"fee_asset"`
		Time    time.Time       `json:"time"`
	} `json:"fills"`
}

func (c *Connector) GetFills(ctx context.Context, query venue.FillQuery) ([]venue.Fill, error) {
	path := "/v1/orders/" + query.VenueOrderID + "/fills"
	body, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	var r fillsResp
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, err
	}
	out := make([]venue.Fill, 0, len(r.Fills))
	for _, f := range r.Fills {
		out = append(out, venue.Fill{
			VenueOrderID: query.VenueOrderID,
			TradeID:      f.TradeID,
			Price:        f.Price,
			Quantity:     f.Qty,
			Fee:          f.Fee,
			FeeAsset:     f.FeeAsset,
			Timestamp:    f.Time.UTC(),
		})
	}
	if query.Limit > 0 && len(out) > query.Limit {
		out = out[:query.Limit]
	}
	return out, nil
}

type balancesResp struct {
	Balances []struct {
		Asset  string          `json:"asset"`
		Free   decimal.Decimal `json:"free"`
		Locked decimal.Decimal `json:"locked"`
	} `json:"balances"`
}

func (c *Connector) GetBalances(ctx context.Context) (*venue.Balances, error) {
	body, err := c.do(ctx, http.MethodGet, "/v1/balances", nil)
	if err != nil {
		return nil, err
	}
	var r balancesResp
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, err
	}
	assets := make(map[string]venue.Balance, len(r.Balances))
	for _, b := range r.Balances {
		assets[b.Asset] = venue.Balance{Asset: b.Asset, Free: b.Free, Locked: b.Locked}
	}
	return &venue.Balances{Assets: assets}, nil
}

func (c *Connector) SubscribeBook(ctx context.Context, pairs []string) (<-chan venue.BookUpdate, error) {
	return nil, ErrUnsupported
}

func (c *Connector) Config() venue.VenueConfig { return c.cfg }

func (c *Connector) do(ctx context.Context, method, path string, payload interface{}) ([]byte, error) {
	if err := c.limiter.Wait(ctx, 1); err != nil {
		return nil, err
	}
	var bodyReader io.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.cfg.RESTBase+path, bodyReader)
	if err != nil {
		return nil, err
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		c.limiter.Backoff("otc", 5*time.Second)
		return nil, fmt.Errorf("otc: rate limited")
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("otc: status %d body %s", resp.StatusCode, string(body))
	}
	return body, nil
}

func (c *Connector) FetchSFTPConfirmation(ctx context.Context, orderID string) ([]byte, error) {
	if c.sftpPath == "" {
		return nil, fmt.Errorf("otc: sftp not configured")
	}
	return fetchSFTP(c.sftpPath, orderID)
}

func mapStatus(s string) venue.OrderStatus {
	switch strings.ToLower(s) {
	case "new", "pending", "open":
		return venue.OrderStatusNew
	case "partial", "partially_filled":
		return venue.OrderStatusPartial
	case "filled", "settled", "completed":
		return venue.OrderStatusFilled
	case "canceled", "cancelled", "rejected":
		return venue.OrderStatusCanceled
	default:
		return venue.OrderStatus(s)
	}
}