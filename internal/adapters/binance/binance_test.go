package binance

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/exchange-connector/internal/ratelimit"
	"github.com/ai-crypto-onramp/exchange-connector/internal/venue"
	"github.com/shopspring/decimal"
)

func newTestConnector(t *testing.T, fn func(w http.ResponseWriter, r *http.Request)) (*Connector, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(fn))
	t.Cleanup(srv.Close)
	c := NewConnector("testkey", "testsecret", ratelimit.NewWeightedLimiter("binance", 1200))
	c.cfg.RESTBase = srv.URL
	return c, srv
}

func TestPlaceOrder(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-MBX-APIKEY") != "testkey" {
			t.Errorf("missing api key header")
		}
		if r.URL.Query().Get("signature") == "" {
			t.Errorf("missing signature")
		}
		if r.URL.Query().Get("timestamp") == "" {
			t.Errorf("missing timestamp")
		}
		if r.URL.Query().Get("recvWindow") == "" {
			t.Errorf("missing recvWindow")
		}
		_, _ = w.Write([]byte(`{"orderId":123456,"clientOrderId":"c1","status":"FILLED","executedQty":"0.5","cummulativeQuoteQty":"25000","fills":[{"price":"50000","qty":"0.5","commission":"5","commissionAsset":"USDT","tradeId":777}]}`))
	})
	resp, err := c.PlaceOrder(context.Background(), venue.OrderRequest{
		ClientOrderID: "c1",
		Symbol:       "BTCUSDT",
		Side:         venue.SideBuy,
		Type:         venue.OrderTypeMarket,
		Quantity:     decimal.NewFromFloat(0.5),
	})
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if resp.VenueOrderID != "123456" {
		t.Fatalf("order id: %s", resp.VenueOrderID)
	}
	if resp.Status != venue.OrderStatusFilled {
		t.Fatalf("status: %s", resp.Status)
	}
	if !resp.FilledQty.Equal(decimal.NewFromFloat(0.5)) {
		t.Fatalf("filled: %v", resp.FilledQty)
	}
	if !resp.AvgPrice.Equal(decimal.NewFromInt(50000)) {
		t.Fatalf("avg: %v", resp.AvgPrice)
	}
	if len(resp.Fills) != 1 || resp.Fills[0].TradeID != "777" {
		t.Fatalf("fills: %+v", resp.Fills)
	}
}

func TestPlaceOrderLimit(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("price") != "51000" {
			t.Errorf("price param: %s", r.URL.Query().Get("price"))
		}
		if r.URL.Query().Get("timeInForce") != "GTC" {
			t.Errorf("tif: %s", r.URL.Query().Get("timeInForce"))
		}
		_, _ = w.Write([]byte(`{"orderId":1,"clientOrderId":"c2","status":"NEW","executedQty":"0","cummulativeQuoteQty":"0"}`))
	})
	resp, err := c.PlaceOrder(context.Background(), venue.OrderRequest{
		ClientOrderID: "c2",
		Symbol:       "BTCUSDT",
		Side:         venue.SideBuy,
		Type:         venue.OrderTypeLimit,
		Quantity:     decimal.NewFromInt(1),
		Price:        decimal.NewFromInt(51000),
	})
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if resp.Status != venue.OrderStatusNew {
		t.Fatalf("status: %s", resp.Status)
	}
}

func TestGetOrder(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("orderId") != "123" {
			t.Errorf("orderId: %s", r.URL.Query().Get("orderId"))
		}
		_, _ = w.Write([]byte(`{"orderId":123,"clientOrderId":"c","status":"PARTIALLY_FILLED","executedQty":"0.25","cummulativeQuoteQty":"12500"}`))
	})
	o, err := c.GetOrder(context.Background(), "123")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if o.Status != venue.OrderStatusPartial {
		t.Fatalf("status: %s", o.Status)
	}
	if !o.FilledQty.Equal(decimal.NewFromFloat(0.25)) {
		t.Fatalf("filled: %v", o.FilledQty)
	}
	if !o.AvgPrice.Equal(decimal.NewFromInt(50000)) {
		t.Fatalf("avg: %v", o.AvgPrice)
	}
}

func TestCancelOrder(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("method: %s", r.Method)
		}
		if r.URL.Query().Get("orderId") != "9" {
			t.Errorf("orderId: %s", r.URL.Query().Get("orderId"))
		}
		_, _ = w.Write([]byte(`{}`))
	})
	if err := c.CancelOrder(context.Background(), venue.CancelRequest{VenueOrderID: "9"}); err != nil {
		t.Fatalf("cancel: %v", err)
	}
}

func TestCancelOrderClientID(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("origClientOrderId") != "c9" {
			t.Errorf("client: %s", r.URL.Query().Get("origClientOrderId"))
		}
		_, _ = w.Write([]byte(`{}`))
	})
	if err := c.CancelOrder(context.Background(), venue.CancelRequest{ClientOrderID: "c9"}); err != nil {
		t.Fatalf("cancel: %v", err)
	}
}

func TestCancelOrderMissingID(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	})
	if err := c.CancelOrder(context.Background(), venue.CancelRequest{}); err == nil {
		t.Fatalf("expected error")
	}
}

func TestGetFills(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("orderId") != "123" {
			t.Errorf("orderId: %s", r.URL.Query().Get("orderId"))
		}
		_, _ = w.Write([]byte(`{"fills":[{"id":1,"price":"50000","qty":"0.5","commission":"5","commissionAsset":"USDT","time":1700000000000}]}`))
	})
	fills, err := c.GetFills(context.Background(), venue.FillQuery{VenueOrderID: "123"})
	if err != nil {
		t.Fatalf("fills: %v", err)
	}
	if len(fills) != 1 {
		t.Fatalf("len: %d", len(fills))
	}
	if !fills[0].Price.Equal(decimal.NewFromInt(50000)) {
		t.Fatalf("price: %v", fills[0].Price)
	}
	if !fills[0].Quantity.Equal(decimal.NewFromFloat(0.5)) {
		t.Fatalf("qty: %v", fills[0].Quantity)
	}
	if fills[0].FeeAsset != "USDT" {
		t.Fatalf("feeAsset: %s", fills[0].FeeAsset)
	}
}

func TestGetBalances(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/api/v3/account") {
			t.Errorf("path: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"balances":[{"asset":"BTC","free":"1.5","locked":"0.1"},{"asset":"USDT","free":"100","locked":"0"}]}`))
	})
	b, err := c.GetBalances(context.Background())
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	if !b.Assets["BTC"].Free.Equal(decimal.NewFromFloat(1.5)) {
		t.Fatalf("btc free: %v", b.Assets["BTC"].Free)
	}
	if !b.Assets["USDT"].Locked.Equal(decimal.Zero) {
		t.Fatalf("usdt locked: %v", b.Assets["USDT"].Locked)
	}
}

func TestRateLimit429(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-MBX-USED-WEIGHT-1M", "1200")
		w.WriteHeader(429)
		_, _ = w.Write([]byte(`{"code":-1003,"msg":"rate limited"}`))
	})
	_, err := c.GetBalances(context.Background())
	if err == nil || !strings.Contains(err.Error(), "rate limited") {
		t.Fatalf("expected rate limit err, got %v", err)
	}
}

func TestSignConsistent(t *testing.T) {
	c := NewConnector("k", "s", ratelimit.NewWeightedLimiter("binance", 1200))
	q := "symbol=BTCUSDT&side=buy&type=market&quantity=0.5&timestamp=123&recvWindow=5000"
	sig := c.sign(q)
	if sig == "" {
		t.Fatalf("empty signature")
	}
	sig2 := c.sign(q)
	if sig != sig2 {
		t.Fatalf("signature not deterministic: %s vs %s", sig, sig2)
	}
}

func TestVenueConfig(t *testing.T) {
	c := NewConnector("k", "s", ratelimit.NewWeightedLimiter("binance", 1200))
	cfg := c.Config()
	if cfg.Name != "binance" {
		t.Fatalf("name: %s", cfg.Name)
	}
	if cfg.RESTBase == "" {
		t.Fatalf("missing rest base")
	}
	if cfg.WSBase == "" {
		t.Fatalf("missing ws base")
	}
}

func TestVenueInterfaceSatisfaction(t *testing.T) {
	var _ venue.VenueConnector = (*Connector)(nil)
}

func TestParseURL(t *testing.T) {
	u, _ := url.Parse("https://api.binance.com/api/v3/order?symbol=BTCUSDT&signature=abc")
	if u.Path != "/api/v3/order" {
		t.Fatalf("path: %s", u.Path)
	}
}

func TestPlaceOrderMissingClientID(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {})
	_, err := c.PlaceOrder(context.Background(), venue.OrderRequest{
		Symbol: "BTCUSDT", Side: venue.SideBuy, Type: venue.OrderTypeMarket, Quantity: decimal.NewFromInt(1),
	})
	if err == nil {
		t.Fatalf("expected error for missing client order id")
	}
}

func TestPlaceOrderBadJSON(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`not-json`))
	})
	_, err := c.PlaceOrder(context.Background(), venue.OrderRequest{
		ClientOrderID: "c1", Symbol: "BTCUSDT", Side: venue.SideBuy, Type: venue.OrderTypeMarket, Quantity: decimal.NewFromInt(1),
	})
	if err == nil {
		t.Fatalf("expected json error")
	}
}

func TestGetOrderBadJSON(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`not-json`))
	})
	_, err := c.GetOrder(context.Background(), "123")
	if err == nil {
		t.Fatalf("expected json error")
	}
}

func TestGetFillsAllParams(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("orderId") != "9" {
			t.Errorf("orderId: %s", q.Get("orderId"))
		}
		if q.Get("startTime") == "" {
			t.Errorf("missing startTime")
		}
		if q.Get("endTime") == "" {
			t.Errorf("missing endTime")
		}
		if q.Get("limit") != "10" {
			t.Errorf("limit: %s", q.Get("limit"))
		}
		_, _ = w.Write([]byte(`{"fills":[]}`))
	})
	start := time.Unix(1700000000, 0)
	end := time.Unix(1700001000, 0)
	fills, err := c.GetFills(context.Background(), venue.FillQuery{
		VenueOrderID: "9", Start: start, End: end, Limit: 10,
	})
	if err != nil {
		t.Fatalf("fills: %v", err)
	}
	if len(fills) != 0 {
		t.Fatalf("expected empty fills, got %d", len(fills))
	}
}

func TestGetFillsDefaultLimit(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("limit") != "500" {
			t.Errorf("default limit: %s", r.URL.Query().Get("limit"))
		}
		_, _ = w.Write([]byte(`{"fills":[]}`))
	})
	fills, err := c.GetFills(context.Background(), venue.FillQuery{})
	if err != nil {
		t.Fatalf("fills: %v", err)
	}
	if len(fills) != 0 {
		t.Fatalf("expected empty fills, got %d", len(fills))
	}
}

func TestGetFillsBadJSON(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`not-json`))
	})
	_, err := c.GetFills(context.Background(), venue.FillQuery{VenueOrderID: "1"})
	if err == nil {
		t.Fatalf("expected json error")
	}
}

func TestGetBalancesBadJSON(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`not-json`))
	})
	_, err := c.GetBalances(context.Background())
	if err == nil {
		t.Fatalf("expected json error")
	}
}

func TestGetBalancesStatus418(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(418)
		_, _ = w.Write([]byte(`{"code":-1003,"msg":"banned"}`))
	})
	_, err := c.GetBalances(context.Background())
	if err == nil || !strings.Contains(err.Error(), "rate limited") {
		t.Fatalf("expected rate limited err, got %v", err)
	}
}

func TestGetBalancesStatus400(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		_, _ = w.Write([]byte(`{"code":-2014,"msg":"bad key"}`))
	})
	_, err := c.GetBalances(context.Background())
	if err == nil || !strings.Contains(err.Error(), "code -2014") {
		t.Fatalf("expected 4xx err, got %v", err)
	}
}

func TestGetBalancesUsedWeightHeader(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-MBX-USED-WEIGHT-1M", "999")
		_, _ = w.Write([]byte(`{"balances":[]}`))
	})
	b, err := c.GetBalances(context.Background())
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	if len(b.Assets) != 0 {
		t.Fatalf("expected no assets, got %d", len(b.Assets))
	}
	if c.limiter.Used() != 999 {
		t.Fatalf("used weight not updated: %d", c.limiter.Used())
	}
}

func TestReadBudgetDefault(t *testing.T) {
	if readBudget() != 1200 {
		t.Fatalf("default budget: %d", readBudget())
	}
}

func TestNormalizeSymbol(t *testing.T) {
	if normalizeSymbol("btcusdt") != "BTCUSDT" {
		t.Fatalf("normalize: %s", normalizeSymbol("btcusdt"))
	}
}

func TestMapStatusAllCases(t *testing.T) {
	cases := []struct {
		in   string
		want venue.OrderStatus
	}{
		{"NEW", venue.OrderStatusNew},
		{"new", venue.OrderStatusNew},
		{"PARTIALLY_FILLED", venue.OrderStatusPartial},
		{"PARTIALLYFILLED", venue.OrderStatusPartial},
		{"FILLED", venue.OrderStatusFilled},
		{"CANCELED", venue.OrderStatusCanceled},
		{"CANCELLED", venue.OrderStatusCanceled},
		{"EXPIRED", venue.OrderStatus("EXPIRED")},
	}
	for _, c := range cases {
		if got := mapStatus(c.in); got != c.want {
			t.Fatalf("mapStatus(%q) = %s, want %s", c.in, got, c.want)
		}
	}
}

func TestMapOrderWithQuoteAvg(t *testing.T) {
	r := placeOrderResp{
		OrderID: 7, ClientOrderID: "c", Status: "FILLED",
		ExecutedQty: "2", CummulativeQuoteQty: "100000",
		Fills: []binanceFill{{Price: "50000", Qty: "1", Commission: "1", CommissionAsset: "USDT", TradeID: 1}},
	}
	o := mapOrder(r)
	if !o.AvgPrice.Equal(decimal.NewFromInt(50000)) {
		t.Fatalf("avg: %v", o.AvgPrice)
	}
	if len(o.Fills) != 1 || o.Fills[0].VenueOrderID != "7" {
		t.Fatalf("fills: %+v", o.Fills)
	}
}

func TestMapOrderNoQuoteUsesFillPrice(t *testing.T) {
	r := placeOrderResp{
		OrderID: 8, ClientOrderID: "c", Status: "FILLED",
		ExecutedQty: "1",
		Fills: []binanceFill{{Price: "51000", Qty: "1", Commission: "0", CommissionAsset: "USDT", TradeID: 2}},
	}
	o := mapOrder(r)
	if !o.AvgPrice.Equal(decimal.NewFromInt(51000)) {
		t.Fatalf("avg from fill: %v", o.AvgPrice)
	}
}

func TestMapOrderSimpleNoExec(t *testing.T) {
	r := orderResp{OrderID: 1, ClientOrderID: "c", Status: "NEW", ExecutedQty: "0", CummulativeQuoteQty: "0"}
	o := mapOrderSimple(r)
	if !o.AvgPrice.Equal(decimal.Zero) {
		t.Fatalf("avg zero: %v", o.AvgPrice)
	}
	if o.Status != venue.OrderStatusNew {
		t.Fatalf("status: %s", o.Status)
	}
}

func TestSubscribeBookNoPairs(t *testing.T) {
	_, err := SubscribeBook(context.Background(), "wss://x", "https://x", nil)
	if err == nil {
		t.Fatalf("expected error for no pairs")
	}
}

func TestStreamToPair(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"btcusdt@depth", "btcusdt"},
		{"btcusdt", "btcusdt"},
		{"@depth", "@depth"},
	}
	for _, c := range cases {
		if got := streamToPair(c.in); got != c.want {
			t.Fatalf("streamToPair(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}