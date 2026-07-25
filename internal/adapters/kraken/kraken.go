package kraken

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ai-crypto-onramp/gateway-exchange/internal/ratelimit"
	"github.com/ai-crypto-onramp/gateway-exchange/internal/venue"
	"github.com/shopspring/decimal"
)

type Connector struct {
	cfg        venue.VenueConfig
	apiKey     string
	apiSecret  string
	limiter    *ratelimit.CounterLimiter
	nonce      uint64
}

func NewConnector(apiKey, apiSecret string, limiter *ratelimit.CounterLimiter) *Connector {
	rest := os.Getenv("KRAKEN_REST_URL")
	if rest == "" {
		rest = "https://api.kraken.com"
	}
	wsBase := os.Getenv("KRAKEN_WS_URL")
	if wsBase == "" {
		wsBase = "wss://ws.kraken.com"
	}
	if limiter == nil {
		limiter = ratelimit.NewCounterLimiter("kraken", 20)
	}
	return &Connector{
		cfg: venue.VenueConfig{
			Name:     "kraken",
			RESTBase: rest,
			WSBase:   wsBase,
		},
		apiKey:    apiKey,
		apiSecret: apiSecret,
		limiter:   limiter,
		nonce:     uint64(time.Now().UnixNano()) / 1_000_000,
	}
}

var _ venue.VenueConnector = (*Connector)(nil)

func (c *Connector) nextNonce() string {
	n := atomic.AddUint64(&c.nonce, 1)
	return strconv.FormatUint(n, 10)
}

func (c *Connector) sign(path, nonce, postData string) string {
	sha := sha256.New()
	sha.Write([]byte(nonce + postData))
	hash := sha.Sum(nil)
	secret, _ := base64.StdEncoding.DecodeString(c.apiSecret)
	mac := hmac.New(sha512.New, secret)
	mac.Write(append([]byte(path), hash...))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func (c *Connector) doPrivate(ctx context.Context, path string, params url.Values, cost int64) ([]byte, error) {
	if params == nil {
		params = url.Values{}
	}
	nonce := c.nextNonce()
	params.Set("nonce", nonce)
	postData := params.Encode()
	sig := c.sign(path, nonce, postData)
	u := c.cfg.RESTBase + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(postData))
	if err != nil {
		return nil, err
	}
	req.Header.Set("API-Key", c.apiKey)
	req.Header.Set("API-Sign", sig)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return c.do(req, cost)
}

func (c *Connector) do(req *http.Request, cost int64) ([]byte, error) {
	if err := c.limiter.Wait(req.Context(), cost); err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == 418 {
		c.limiter.Backoff("kraken", 30*time.Second)
		return nil, fmt.Errorf("kraken: rate limited (status %d)", resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("kraken: status %d body %s", resp.StatusCode, string(body))
	}
	var env envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("kraken: bad json: %w", err)
	}
	if len(env.Error) > 0 {
		return nil, fmt.Errorf("kraken: %s", strings.Join(env.Error, "; "))
	}
	return env.Result, nil
}

type envelope struct {
	Error  []string        `json:"error"`
	Result json.RawMessage `json:"result"`
}

func (c *Connector) PlaceOrder(ctx context.Context, req venue.OrderRequest) (*venue.OrderResponse, error) {
	if req.ClientOrderID == "" {
		return nil, fmt.Errorf("kraken: client_order_id required")
	}
	params := url.Values{}
	params.Set("pair", normalizePair(req.Symbol))
	params.Set("type", string(req.Side))
	if req.Type == venue.OrderTypeMarket {
		params.Set("ordertype", "market")
	} else {
		params.Set("ordertype", "limit")
		params.Set("price", req.Price.String())
	}
	params.Set("volume", req.Quantity.String())
	params.Set("userref", req.ClientOrderID)
	body, err := c.doPrivate(ctx, "/0/private/AddOrder", params, 6)
	if err != nil {
		return nil, err
	}
	var r addOrderResp
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, err
	}
	if len(r.TxIDs) == 0 {
		return nil, fmt.Errorf("kraken: no txid")
	}
	return &venue.OrderResponse{
		VenueOrderID:  r.TxIDs[0],
		ClientOrderID: req.ClientOrderID,
		Status:        venue.OrderStatusNew,
		FilledQty:     decimal.Zero,
		AvgPrice:      decimal.Zero,
	}, nil
}

type addOrderResp struct {
	TxIDs []string `json:"txid"`
}

func (c *Connector) GetOrder(ctx context.Context, venueOrderID string) (*venue.OrderResponse, error) {
	params := url.Values{}
	params.Set("txid", venueOrderID)
	body, err := c.doPrivate(ctx, "/0/private/QueryOrders", params, 2)
	if err != nil {
		return nil, err
	}
	var m map[string]krakenOrder
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, err
	}
	o, ok := m[venueOrderID]
	if !ok {
		return nil, fmt.Errorf("kraken: order not found")
	}
	return mapOrder(venueOrderID, o), nil
}

type krakenOrder struct {
	Status     string `json:"status"`
	Vol        string `json:"vol"`
	VolExec    string `json:"vol_exec"`
	Price      string `json:"price"`
	UserRef    string `json:"userref"`
}

func mapOrder(id string, o krakenOrder) *venue.OrderResponse {
	exec, _ := decimal.NewFromString(o.VolExec)
	vol, _ := decimal.NewFromString(o.Vol)
	price, _ := decimal.NewFromString(o.Price)
	avg := decimal.Zero
	if !exec.IsZero() {
		avg = price
	}
	status := venue.OrderStatusNew
	switch strings.ToLower(o.Status) {
	case "pending":
		status = venue.OrderStatusNew
	case "open":
		status = venue.OrderStatusNew
	case "closed":
		if exec.Equal(vol) {
			status = venue.OrderStatusFilled
		} else if exec.GreaterThan(decimal.Zero) {
			status = venue.OrderStatusPartial
		} else {
			status = venue.OrderStatusCanceled
		}
	case "canceled", "cancelled":
		status = venue.OrderStatusCanceled
	}
	return &venue.OrderResponse{
		VenueOrderID:  id,
		ClientOrderID: o.UserRef,
		Status:        status,
		FilledQty:     exec,
		AvgPrice:      avg,
	}
}

func (c *Connector) CancelOrder(ctx context.Context, req venue.CancelRequest) error {
	params := url.Values{}
	if req.VenueOrderID != "" {
		params.Set("txid", req.VenueOrderID)
	} else if req.ClientOrderID != "" {
		params.Set("userref", req.ClientOrderID)
	} else {
		return fmt.Errorf("kraken: cancel requires venue or client order id")
	}
	_, err := c.doPrivate(ctx, "/0/private/CancelOrder", params, 6)
	return err
}

type tradeEntry struct {
	OrderTxID string `json:"ordertxid"`
	Pair      string `json:"pair"`
	Time      float64 `json:"time"`
	Type      string `json:"type"`
	Price     string `json:"price"`
	Vol       string `json:"vol"`
	Fee       string `json:"fee"`
	FeeAsset  string `json:"fee_currency"`
}

func (c *Connector) GetFills(ctx context.Context, query venue.FillQuery) ([]venue.Fill, error) {
	params := url.Values{}
	if query.VenueOrderID != "" {
		params.Set("txid", query.VenueOrderID)
	}
	if !query.Start.IsZero() {
		params.Set("start", strconv.FormatInt(query.Start.Unix(), 10))
	}
	if !query.End.IsZero() {
		params.Set("end", strconv.FormatInt(query.End.Unix(), 10))
	}
	body, err := c.doPrivate(ctx, "/0/private/QueryTrades", params, 2)
	if err != nil {
		return nil, err
	}
	var r map[string]tradeEntry
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, err
	}
	out := make([]venue.Fill, 0, len(r))
	for id, tr := range r {
		p, _ := decimal.NewFromString(tr.Price)
		v, _ := decimal.NewFromString(tr.Vol)
		fee, _ := decimal.NewFromString(tr.Fee)
		out = append(out, venue.Fill{
			VenueOrderID: tr.OrderTxID,
			TradeID:      id,
			Price:        p,
			Quantity:     v,
			Fee:          fee,
			FeeAsset:     tr.FeeAsset,
			Timestamp:    time.Unix(int64(tr.Time), 0).UTC(),
		})
	}
	if query.Limit > 0 && len(out) > query.Limit {
		out = out[:query.Limit]
	}
	return out, nil
}

type balanceResp map[string]struct {
	Available string `json:"available"`
	Balance   string `json:"balance"`
}

func (c *Connector) GetBalances(ctx context.Context) (*venue.Balances, error) {
	body, err := c.doPrivate(ctx, "/0/private/Balance", nil, 2)
	if err != nil {
		return nil, err
	}
	var r balanceResp
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, err
	}
	assets := make(map[string]venue.Balance, len(r))
	for asset, b := range r {
		free, _ := decimal.NewFromString(b.Available)
		total, _ := decimal.NewFromString(b.Balance)
		locked := total.Sub(free)
		if locked.LessThan(decimal.Zero) {
			locked = decimal.Zero
		}
		assets[asset] = venue.Balance{Asset: asset, Free: free, Locked: locked}
	}
	return &venue.Balances{Assets: assets}, nil
}

func (c *Connector) SubscribeBook(ctx context.Context, pairs []string) (<-chan venue.BookUpdate, error) {
	return SubscribeBook(ctx, c.cfg.WSBase, pairs)
}

func (c *Connector) Config() venue.VenueConfig { return c.cfg }

func normalizePair(s string) string {
	return s
}

func signExample(secret, path, nonce, postData string) string {
	sha := sha256.New()
	sha.Write([]byte(nonce + postData))
	hash := sha.Sum(nil)
	sec, _ := base64.StdEncoding.DecodeString(secret)
	mac := hmac.New(sha512.New, sec)
	mac.Write(append([]byte(path), hash...))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

var _ = bytes.NewReader