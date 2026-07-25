package kraken

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ai-crypto-onramp/exchange-connector/internal/ratelimit"
	"github.com/ai-crypto-onramp/exchange-connector/internal/venue"
	"github.com/shopspring/decimal"
)

func newTestConnector(t *testing.T, fn func(w http.ResponseWriter, r *http.Request)) (*Connector, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(fn))
	t.Cleanup(srv.Close)
	secret := base64.StdEncoding.EncodeToString([]byte("test-secret-bytes"))
	c := NewConnector("testkey", secret, ratelimit.NewCounterLimiter("kraken", 20))
	c.cfg.RESTBase = srv.URL
	return c, srv
}

func TestPlaceOrder(t *testing.T) {
	var gotNonce string
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("API-Key") != "testkey" {
			t.Errorf("api key: %s", r.Header.Get("API-Key"))
		}
		if r.Header.Get("API-Sign") == "" {
			t.Errorf("missing sign")
		}
		_ = r.ParseForm()
		gotNonce = r.Form.Get("nonce")
		if gotNonce == "" {
			t.Errorf("missing nonce")
		}
		if r.Form.Get("ordertype") != "market" {
			t.Errorf("ordertype: %s", r.Form.Get("ordertype"))
		}
		_, _ = w.Write([]byte(`{"error":[],"result":{"txid":["ABC123"]}}`))
	})
	resp, err := c.PlaceOrder(context.Background(), venue.OrderRequest{
		ClientOrderID: "c1",
		Symbol:        "XBTUSDT",
		Side:          venue.SideBuy,
		Type:          venue.OrderTypeMarket,
		Quantity:      decimal.NewFromFloat(0.5),
	})
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if resp.VenueOrderID != "ABC123" {
		t.Fatalf("txid: %s", resp.VenueOrderID)
	}
	if resp.Status != venue.OrderStatusNew {
		t.Fatalf("status: %s", resp.Status)
	}
}

func TestPlaceOrderLimit(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.Form.Get("price") != "51000" {
			t.Errorf("price: %s", r.Form.Get("price"))
		}
		_, _ = w.Write([]byte(`{"error":[],"result":{"txid":["T1"]}}`))
	})
	resp, err := c.PlaceOrder(context.Background(), venue.OrderRequest{
		ClientOrderID: "c2",
		Symbol:        "XBTUSDT",
		Side:          venue.SideBuy,
		Type:          venue.OrderTypeLimit,
		Quantity:      decimal.NewFromInt(1),
		Price:         decimal.NewFromInt(51000),
	})
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if resp.VenueOrderID != "T1" {
		t.Fatalf("txid: %s", resp.VenueOrderID)
	}
}

func TestGetOrder(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/0/private/QueryOrders") {
			t.Errorf("path: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"error":[],"result":{"ABC":{"status":"closed","vol":"1","vol_exec":"1","price":"50000","userref":"c1"}}}`))
	})
	o, err := c.GetOrder(context.Background(), "ABC")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if o.Status != venue.OrderStatusFilled {
		t.Fatalf("status: %s", o.Status)
	}
	if !o.FilledQty.Equal(decimal.NewFromInt(1)) {
		t.Fatalf("filled: %v", o.FilledQty)
	}
	if !o.AvgPrice.Equal(decimal.NewFromInt(50000)) {
		t.Fatalf("avg: %v", o.AvgPrice)
	}
	if o.ClientOrderID != "c1" {
		t.Fatalf("client: %s", o.ClientOrderID)
	}
}

func TestGetOrderPartial(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"error":[],"result":{"X":{"status":"closed","vol":"2","vol_exec":"0.5","price":"50000"}}}`))
	})
	o, _ := c.GetOrder(context.Background(), "X")
	if o.Status != venue.OrderStatusPartial {
		t.Fatalf("status: %s", o.Status)
	}
}

func TestGetOrderCanceled(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"error":[],"result":{"X":{"status":"canceled","vol":"2","vol_exec":"0","price":"50000"}}}`))
	})
	o, _ := c.GetOrder(context.Background(), "X")
	if o.Status != venue.OrderStatusCanceled {
		t.Fatalf("status: %s", o.Status)
	}
}

func TestCancelOrder(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/0/private/CancelOrder") {
			t.Errorf("path: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"error":[],"result":{"count":1}}`))
	})
	if err := c.CancelOrder(context.Background(), venue.CancelRequest{VenueOrderID: "ABC"}); err != nil {
		t.Fatalf("cancel: %v", err)
	}
}

func TestCancelOrderMissingID(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"error":[],"result":{}}`))
	})
	if err := c.CancelOrder(context.Background(), venue.CancelRequest{}); err == nil {
		t.Fatalf("expected error")
	}
}

func TestGetFills(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"error":[],"result":{"T1":{"ordertxid":"ABC","pair":"XBTUSDT","time":1700000000,"type":"buy","price":"50000","vol":"0.5","fee":"5","fee_currency":"USDT"}}}`))
	})
	fills, err := c.GetFills(context.Background(), venue.FillQuery{VenueOrderID: "ABC"})
	if err != nil {
		t.Fatalf("fills: %v", err)
	}
	if len(fills) != 1 {
		t.Fatalf("len: %d", len(fills))
	}
	if !fills[0].Price.Equal(decimal.NewFromInt(50000)) {
		t.Fatalf("price: %v", fills[0].Price)
	}
	if fills[0].VenueOrderID != "ABC" {
		t.Fatalf("order: %s", fills[0].VenueOrderID)
	}
	if fills[0].FeeAsset != "USDT" {
		t.Fatalf("fee asset: %s", fills[0].FeeAsset)
	}
}

func TestGetBalances(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"error":[],"result":{"XXBT":{"available":"1.5","balance":"1.6"},"ZUSDT":{"available":"100","balance":"100"}}}`))
	})
	b, err := c.GetBalances(context.Background())
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	if !b.Assets["XXBT"].Free.Equal(decimal.NewFromFloat(1.5)) {
		t.Fatalf("btc free: %v", b.Assets["XXBT"].Free)
	}
	if !b.Assets["XXBT"].Locked.Equal(decimal.NewFromFloat(0.1)) {
		t.Fatalf("btc locked: %v", b.Assets["XXBT"].Locked)
	}
}

func TestErrorEnvelope(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"error":["EAPI:Invalid key"],"result":{}}`))
	})
	_, err := c.GetBalances(context.Background())
	if err == nil || !strings.Contains(err.Error(), "EAPI") {
		t.Fatalf("expected error, got %v", err)
	}
}

func TestRateLimit429(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
		_, _ = w.Write([]byte(`{"error":[],"result":{}}`))
	})
	_, err := c.GetBalances(context.Background())
	if err == nil || !strings.Contains(err.Error(), "rate limited") {
		t.Fatalf("expected rate limit, got %v", err)
	}
}

func TestNonceIncrements(t *testing.T) {
	c := NewConnector("k", base64.StdEncoding.EncodeToString([]byte("s")), ratelimit.NewCounterLimiter("kraken", 20))
	n1 := c.nextNonce()
	n2 := c.nextNonce()
	if n1 == n2 {
		t.Fatalf("nonce not incrementing: %s == %s", n1, n2)
	}
}

func TestSignDeterministic(t *testing.T) {
	secret := base64.StdEncoding.EncodeToString([]byte("test-secret-bytes"))
	sig1 := signExample(secret, "/0/private/Balance", "123", "nonce=123")
	sig2 := signExample(secret, "/0/private/Balance", "123", "nonce=123")
	if sig1 != sig2 {
		t.Fatalf("signature not deterministic")
	}
	if sig1 == "" {
		t.Fatalf("empty sig")
	}
}

func TestConfig(t *testing.T) {
	c := NewConnector("k", base64.StdEncoding.EncodeToString([]byte("s")), nil)
	if c.Config().Name != "kraken" {
		t.Fatalf("name: %s", c.Config().Name)
	}
}

func TestVenueInterfaceSatisfaction(t *testing.T) {
	var _ venue.VenueConnector = (*Connector)(nil)
}

func TestEnvelopeParse(t *testing.T) {
	var env envelope
	if err := json.Unmarshal([]byte(`{"error":["a"],"result":{"x":1}}`), &env); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(env.Error) != 1 || env.Error[0] != "a" {
		t.Fatalf("error: %v", env.Error)
	}
}