package binance

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ai-crypto-onramp/gateway-exchange/internal/ratelimit"
	"github.com/ai-crypto-onramp/gateway-exchange/internal/venue"
	"github.com/shopspring/decimal"
)

type Connector struct {
	cfg       venue.VenueConfig
	apiKey    string
	apiSecret string
	limiter   *ratelimit.WeightedLimiter
	mu        sync.Mutex
	nonce     int64
}

func NewConnector(apiKey, apiSecret string, limiter *ratelimit.WeightedLimiter) *Connector {
	rest := os.Getenv("BINANCE_REST_URL")
	if rest == "" {
		rest = "https://api.binance.com"
	}
	wsBase := os.Getenv("BINANCE_WS_URL")
	if wsBase == "" {
		wsBase = "wss://stream.binance.com:9443/ws"
	}
	budget := readBudget()
	if limiter == nil {
		limiter = ratelimit.NewWeightedLimiter("binance", budget)
	}
	return &Connector{
		cfg: venue.VenueConfig{
			Name:     "binance",
			RESTBase: rest,
			WSBase:   wsBase,
		},
		apiKey:    apiKey,
		apiSecret: apiSecret,
		limiter:   limiter,
		nonce:     time.Now().UnixMilli(),
	}
}

func readBudget() int64 {
	b := os.Getenv("RATE_LIMIT_WEIGHT_BUDGET")
	if b == "" {
		return 1200
	}
	n, err := strconv.ParseInt(b, 10, 64)
	if err != nil {
		return 1200
	}
	return n
}

var _ venue.VenueConnector = (*Connector)(nil)

type restError struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

func (c *Connector) sign(query string) string {
	mac := hmac.New(sha256.New, []byte(c.apiSecret))
	mac.Write([]byte(query))
	return hex.EncodeToString(mac.Sum(nil))
}

func (c *Connector) doSigned(ctx context.Context, method, path string, params url.Values, weight int64) ([]byte, http.Header, error) {
	if params == nil {
		params = url.Values{}
	}
	c.mu.Lock()
	c.nonce++
	ts := c.nonce
	c.mu.Unlock()
	params.Set("timestamp", strconv.FormatInt(ts, 10))
	params.Set("recvWindow", "5000")
	query := params.Encode()
	sig := c.sign(query)
	full := query + "&signature=" + sig
	u := c.cfg.RESTBase + path + "?" + full
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("X-MBX-APIKEY", c.apiKey)
	return c.do(req, weight)
}

func (c *Connector) do(req *http.Request, weight int64) ([]byte, http.Header, error) {
	if err := c.limiter.Wait(req.Context(), weight); err != nil {
		return nil, nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	if h := resp.Header.Get("X-MBX-USED-WEIGHT-1M"); h != "" {
		if used, err := strconv.ParseInt(h, 10, 64); err == nil {
			c.limiter.UpdateUsed("binance", used)
		}
	}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == 418 {
		c.limiter.Backoff("binance", 30*time.Second)
		return nil, resp.Header, fmt.Errorf("binance: rate limited (status %d)", resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		var re restError
		_ = json.Unmarshal(body, &re)
		return nil, resp.Header, fmt.Errorf("binance: status %d code %d msg %s", resp.StatusCode, re.Code, re.Msg)
	}
	return body, resp.Header, nil
}

type placeOrderResp struct {
	OrderID       int64         `json:"orderId"`
	ClientOrderID string        `json:"clientOrderId"`
	Status         string        `json:"status"`
	ExecutedQty    string        `json:"executedQty"`
	CummulativeQuoteQty string   `json:"cummulativeQuoteQty"`
	Fills         []binanceFill `json:"fills"`
	Symbol        string        `json:"symbol"`
}

type binanceFill struct {
	Price       string `json:"price"`
	Qty         string `json:"qty"`
	Commission  string `json:"commission"`
	CommissionAsset string `json:"commissionAsset"`
	TradeID     int64  `json:"tradeId"`
}

func (c *Connector) PlaceOrder(ctx context.Context, req venue.OrderRequest) (*venue.OrderResponse, error) {
	if req.ClientOrderID == "" {
		return nil, fmt.Errorf("binance: client_order_id required")
	}
	params := url.Values{}
	params.Set("symbol", normalizeSymbol(req.Symbol))
	params.Set("side", string(req.Side))
	params.Set("type", string(req.Type))
	params.Set("quantity", req.Quantity.String())
	params.Set("newClientOrderId", req.ClientOrderID)
	if req.Type == venue.OrderTypeLimit {
		params.Set("price", req.Price.String())
		params.Set("timeInForce", "GTC")
	}
	body, _, err := c.doSigned(ctx, http.MethodPost, "/api/v3/order", params, 1)
	if err != nil {
		return nil, err
	}
	var r placeOrderResp
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, err
	}
	return mapOrder(r), nil
}

type orderResp struct {
	OrderID       int64         `json:"orderId"`
	ClientOrderID string        `json:"clientOrderId"`
	Status        string        `json:"status"`
	ExecutedQty    string        `json:"executedQty"`
	CummulativeQuoteQty string `json:"cummulativeQuoteQty"`
	Symbol        string        `json:"symbol"`
}

func (c *Connector) GetOrder(ctx context.Context, venueOrderID string) (*venue.OrderResponse, error) {
	params := url.Values{}
	params.Set("orderId", venueOrderID)
	body, _, err := c.doSigned(ctx, http.MethodGet, "/api/v3/order", params, 1)
	if err != nil {
		return nil, err
	}
	var r orderResp
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, err
	}
	return mapOrderSimple(r), nil
}

func (c *Connector) CancelOrder(ctx context.Context, req venue.CancelRequest) error {
	params := url.Values{}
	if req.VenueOrderID != "" {
		params.Set("orderId", req.VenueOrderID)
	} else if req.ClientOrderID != "" {
		params.Set("origClientOrderId", req.ClientOrderID)
	} else {
		return fmt.Errorf("binance: cancel requires venue or client order id")
	}
	_, _, err := c.doSigned(ctx, http.MethodDelete, "/api/v3/order", params, 1)
	return err
}

type fillsResp struct {
	Fills []struct {
		TradeID        int64  `json:"id"`
		Price          string `json:"price"`
		Qty            string `json:"qty"`
		Commission     string `json:"commission"`
		CommissionAsset string `json:"commissionAsset"`
		Time           int64  `json:"time"`
	} `json:"fills"`
}

func (c *Connector) GetFills(ctx context.Context, query venue.FillQuery) ([]venue.Fill, error) {
	params := url.Values{}
	if query.VenueOrderID != "" {
		params.Set("orderId", query.VenueOrderID)
	}
	if !query.Start.IsZero() {
		params.Set("startTime", strconv.FormatInt(query.Start.UnixMilli(), 10))
	}
	if !query.End.IsZero() {
		params.Set("endTime", strconv.FormatInt(query.End.UnixMilli(), 10))
	}
	if query.Limit > 0 {
		params.Set("limit", strconv.Itoa(query.Limit))
	} else {
		params.Set("limit", "500")
	}
	body, _, err := c.doSigned(ctx, http.MethodGet, "/api/v3/myTrades", params, 10)
	if err != nil {
		return nil, err
	}
	var r fillsResp
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, err
	}
	out := make([]venue.Fill, 0, len(r.Fills))
	for _, f := range r.Fills {
		p, _ := decimal.NewFromString(f.Price)
		q, _ := decimal.NewFromString(f.Qty)
		fee, _ := decimal.NewFromString(f.Commission)
		out = append(out, venue.Fill{
			VenueOrderID: query.VenueOrderID,
			TradeID:      strconv.FormatInt(f.TradeID, 10),
			Price:        p,
			Quantity:     q,
			Fee:          fee,
			FeeAsset:     f.CommissionAsset,
			Timestamp:    time.UnixMilli(f.Time).UTC(),
		})
	}
	return out, nil
}

type accountResp struct {
	Balances []struct {
		Asset  string `json:"asset"`
		Free   string `json:"free"`
		Locked string `json:"locked"`
	} `json:"balances"`
}

func (c *Connector) GetBalances(ctx context.Context) (*venue.Balances, error) {
	body, _, err := c.doSigned(ctx, http.MethodGet, "/api/v3/account", nil, 5)
	if err != nil {
		return nil, err
	}
	var r accountResp
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, err
	}
	assets := make(map[string]venue.Balance, len(r.Balances))
	for _, b := range r.Balances {
		free, _ := decimal.NewFromString(b.Free)
		locked, _ := decimal.NewFromString(b.Locked)
		assets[b.Asset] = venue.Balance{Asset: b.Asset, Free: free, Locked: locked}
	}
	return &venue.Balances{Assets: assets}, nil
}

func (c *Connector) SubscribeBook(ctx context.Context, pairs []string) (<-chan venue.BookUpdate, error) {
	return SubscribeBook(ctx, c.cfg.WSBase, c.cfg.RESTBase, pairs)
}

func (c *Connector) Config() venue.VenueConfig { return c.cfg }

func normalizeSymbol(s string) string {
	return strings.ToUpper(s)
}

func mapOrder(r placeOrderResp) *venue.OrderResponse {
	exec, _ := decimal.NewFromString(r.ExecutedQty)
	avg := decimal.Zero
	if r.CummulativeQuoteQty != "" && !exec.IsZero() {
		quote, _ := decimal.NewFromString(r.CummulativeQuoteQty)
		avg = quote.Div(exec)
	}
	fills := make([]venue.Fill, 0, len(r.Fills))
	for _, f := range r.Fills {
		p, _ := decimal.NewFromString(f.Price)
		q, _ := decimal.NewFromString(f.Qty)
		fee, _ := decimal.NewFromString(f.Commission)
		fills = append(fills, venue.Fill{
			VenueOrderID: strconv.FormatInt(r.OrderID, 10),
			TradeID:      strconv.FormatInt(f.TradeID, 10),
			Price:        p,
			Quantity:     q,
			Fee:          fee,
			FeeAsset:     f.CommissionAsset,
			Timestamp:    time.Now().UTC(),
		})
	}
	if avg.IsZero() && len(fills) > 0 {
		avg = fills[0].Price
	}
	return &venue.OrderResponse{
		VenueOrderID:  strconv.FormatInt(r.OrderID, 10),
		ClientOrderID: r.ClientOrderID,
		Status:        mapStatus(r.Status),
		FilledQty:     exec,
		AvgPrice:      avg,
		Fills:         fills,
	}
}

func mapOrderSimple(r orderResp) *venue.OrderResponse {
	exec, _ := decimal.NewFromString(r.ExecutedQty)
	quote, _ := decimal.NewFromString(r.CummulativeQuoteQty)
	avg := decimal.Zero
	if !exec.IsZero() {
		avg = quote.Div(exec)
	}
	return &venue.OrderResponse{
		VenueOrderID:  strconv.FormatInt(r.OrderID, 10),
		ClientOrderID: r.ClientOrderID,
		Status:        mapStatus(r.Status),
		FilledQty:     exec,
		AvgPrice:      avg,
	}
}

func mapStatus(s string) venue.OrderStatus {
	switch strings.ToUpper(s) {
	case "NEW":
		return venue.OrderStatusNew
	case "PARTIALLY_FILLED", "PARTIALLYFILLED":
		return venue.OrderStatusPartial
	case "FILLED":
		return venue.OrderStatusFilled
	case "CANCELED", "CANCELLED":
		return venue.OrderStatusCanceled
	default:
		return venue.OrderStatus(s)
	}
}