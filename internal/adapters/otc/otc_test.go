package otc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ai-crypto-onramp/gateway-exchange/internal/ratelimit"
	"github.com/ai-crypto-onramp/gateway-exchange/internal/venue"
	"github.com/shopspring/decimal"
)

func newTestConnector(t *testing.T, fn func(w http.ResponseWriter, r *http.Request)) (*Connector, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(fn))
	t.Cleanup(srv.Close)
	c := NewConnector("testkey", ratelimit.NewRPSLimiter("otc", 100))
	c.cfg.RESTBase = srv.URL
	return c, srv
}

func TestPlaceOrder(t *testing.T) {
	calls := 0
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer testkey" {
			t.Errorf("auth: %s", r.Header.Get("Authorization"))
		}
		if r.URL.Path == "/v1/rfq" {
			_, _ = w.Write([]byte(`{"quote_id":"Q1","price":"50000","expires_at":"2025-01-01T00:00:00Z","status":"open"}`))
			return
		}
		if r.URL.Path == "/v1/quotes/Q1/accept" {
			_, _ = w.Write([]byte(`{"order_id":"O1","status":"filled","filled_qty":"0.5","price":"50000","fee":"5","fee_asset":"USDT","trade_id":"T1"}`))
			return
		}
		w.WriteHeader(404)
	})
	resp, err := c.PlaceOrder(context.Background(), venue.OrderRequest{
		ClientOrderID: "c1",
		Symbol:        "BTCUSDT",
		Side:          venue.SideBuy,
		Type:          venue.OrderTypeMarket,
		Quantity:      decimal.NewFromFloat(0.5),
	})
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	if resp.VenueOrderID != "O1" {
		t.Fatalf("order: %s", resp.VenueOrderID)
	}
	if resp.Status != venue.OrderStatusFilled {
		t.Fatalf("status: %s", resp.Status)
	}
	if !resp.FilledQty.Equal(decimal.NewFromFloat(0.5)) {
		t.Fatalf("filled: %v", resp.FilledQty)
	}
	if len(resp.Fills) != 1 || resp.Fills[0].TradeID != "T1" {
		t.Fatalf("fills: %+v", resp.Fills)
	}
	if calls != 2 {
		t.Fatalf("expected 2 calls, got %d", calls)
	}
}

func TestPlaceOrderQuoteRejected(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"quote_id":"Q1","price":"50000","status":"rejected"}`))
	})
	_, err := c.PlaceOrder(context.Background(), venue.OrderRequest{
		ClientOrderID: "c1",
		Symbol:        "BTCUSDT",
		Side:          venue.SideBuy,
		Type:          venue.OrderTypeMarket,
		Quantity:      decimal.NewFromFloat(0.5),
	})
	if err == nil || !strings.Contains(err.Error(), "rejected") {
		t.Fatalf("expected rejected, got %v", err)
	}
}

func TestPlaceOrderMissingClientID(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {})
	_, err := c.PlaceOrder(context.Background(), venue.OrderRequest{
		Symbol: "BTCUSDT", Side: venue.SideBuy, Type: venue.OrderTypeMarket, Quantity: decimal.NewFromInt(1),
	})
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestGetOrder(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/orders/O1" {
			t.Errorf("path: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"order_id":"O1","status":"new","filled_qty":"0","price":"0","settlement_status":"pending"}`))
	})
	o, err := c.GetOrder(context.Background(), "O1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if o.Status != venue.OrderStatusNew {
		t.Fatalf("status: %s", o.Status)
	}
}

func TestGetOrderSettled(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"order_id":"O1","status":"new","filled_qty":"1","price":"50000","settlement_status":"settled"}`))
	})
	o, _ := c.GetOrder(context.Background(), "O1")
	if o.Status != venue.OrderStatusFilled {
		t.Fatalf("status: %s", o.Status)
	}
}

func TestCancelOrder(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/orders/O1/cancel" || r.Method != http.MethodPost {
			t.Errorf("path/method: %s %s", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"status":"canceled"}`))
	})
	if err := c.CancelOrder(context.Background(), venue.CancelRequest{VenueOrderID: "O1"}); err != nil {
		t.Fatalf("cancel: %v", err)
	}
}

func TestCancelOrderMissingID(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {})
	if err := c.CancelOrder(context.Background(), venue.CancelRequest{}); err == nil {
		t.Fatalf("expected error")
	}
}

func TestGetFills(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/orders/O1/fills" {
			t.Errorf("path: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"fills":[{"trade_id":"T1","price":"50000","qty":"0.5","fee":"5","fee_asset":"USDT","time":"2025-01-01T00:00:00Z"}]}`))
	})
	fills, err := c.GetFills(context.Background(), venue.FillQuery{VenueOrderID: "O1"})
	if err != nil {
		t.Fatalf("fills: %v", err)
	}
	if len(fills) != 1 || fills[0].TradeID != "T1" {
		t.Fatalf("fills: %+v", fills)
	}
	if !fills[0].Price.Equal(decimal.NewFromInt(50000)) {
		t.Fatalf("price: %v", fills[0].Price)
	}
}

func TestGetBalances(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"balances":[{"asset":"BTC","free":"1.5","locked":"0.1"},{"asset":"USDT","free":"100","locked":"0"}]}`))
	})
	b, err := c.GetBalances(context.Background())
	if err != nil {
		t.Fatalf("balances: %v", err)
	}
	if !b.Assets["BTC"].Free.Equal(decimal.NewFromFloat(1.5)) {
		t.Fatalf("btc: %v", b.Assets["BTC"].Free)
	}
}

func TestSubscribeBookUnsupported(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {})
	_, err := c.SubscribeBook(context.Background(), []string{"BTCUSDT"})
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("expected ErrUnsupported, got %v", err)
	}
}

func TestRateLimit429(t *testing.T) {
	c, _ := newTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
	})
	_, err := c.GetBalances(context.Background())
	if err == nil || !strings.Contains(err.Error(), "rate limited") {
		t.Fatalf("expected rate limit, got %v", err)
	}
}

func TestSFTPConfirmation(t *testing.T) {
	dir := t.TempDir()
	confirm := []byte(`{"order_id":"O1","settlement":"confirmed"}`)
	if err := os.WriteFile(filepath.Join(dir, "O1.json"), confirm, 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	c := NewConnector("k", ratelimit.NewRPSLimiter("otc", 100))
	c.sftpPath = dir
	data, err := c.FetchSFTPConfirmation(context.Background(), "O1")
	if err != nil {
		t.Fatalf("sftp: %v", err)
	}
	if string(data) != string(confirm) {
		t.Fatalf("data: %s", data)
	}
}

func TestSFTPNotConfigured(t *testing.T) {
	c := NewConnector("k", ratelimit.NewRPSLimiter("otc", 100))
	_, err := c.FetchSFTPConfirmation(context.Background(), "O1")
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestConfig(t *testing.T) {
	c := NewConnector("k", nil)
	if c.Config().Name != "otc" {
		t.Fatalf("name: %s", c.Config().Name)
	}
}

func TestVenueInterfaceSatisfaction(t *testing.T) {
	var _ venue.VenueConnector = (*Connector)(nil)
}