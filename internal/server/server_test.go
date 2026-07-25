package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/exchange-connector/internal/audit"
	"github.com/ai-crypto-onramp/exchange-connector/internal/venue"
	"github.com/shopspring/decimal"
)

func newTestService(t *testing.T) (*Service, *venue.DummyVenueConnector, *audit.InMemorySink) {
	t.Helper()
	conn := venue.NewDummyVenueConnector()
	sink := audit.NewInMemorySink()
	svc, err := NewService(conn, sink, Config{VenueName: "dummy", Pairs: []string{"BTCUSDT"}}, nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	return svc, conn, sink
}

func TestHealthz(t *testing.T) {
	svc, _, _ := newTestService(t)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("status: %s", body["status"])
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type: %s", ct)
	}
}

func TestReadyz(t *testing.T) {
	svc, _, _ := newTestService(t)
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestAdminStatusNoFill(t *testing.T) {
	svc, _, _ := newTestService(t)
	req := httptest.NewRequest(http.MethodGet, "/admin/status", nil)
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var st adminStatus
	if err := json.NewDecoder(rec.Body).Decode(&st); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if st.CircuitBreaker != "closed" {
		t.Fatalf("cb: %s", st.CircuitBreaker)
	}
	if st.RateLimitHeadroom != "unlimited" {
		t.Fatalf("headroom: %s", st.RateLimitHeadroom)
	}
	if st.Venue != "dummy" {
		t.Fatalf("venue: %s", st.Venue)
	}
	if st.LastFill != nil {
		t.Fatalf("expected nil last fill")
	}
}

func TestAdminStatusWithFill(t *testing.T) {
	svc, conn, _ := newTestService(t)
	_, _ = conn.PlaceOrder(context.Background(), venue.OrderRequest{
		ClientOrderID: "x1", Symbol: "BTCUSDT", Side: venue.SideBuy, Type: venue.OrderTypeMarket, Quantity: decimal.NewFromInt(1),
	})
	req := httptest.NewRequest(http.MethodGet, "/admin/status", nil)
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	var st adminStatus
	_ = json.NewDecoder(rec.Body).Decode(&st)
	if st.LastFill == nil || st.LastFill.VenueOrderID != "x1" {
		t.Fatalf("last fill: %+v", st.LastFill)
	}
}

func TestRotateCredentials(t *testing.T) {
	svc, _, _ := newTestService(t)
	req := httptest.NewRequest(http.MethodPost, "/admin/rotate-credentials", nil)
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestPlaceOrderSuccess(t *testing.T) {
	svc, _, sink := newTestService(t)
	body := `{"venue":"dummy","pair":"BTCUSDT","side":"buy","type":"market","amount":"0.5"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/orders", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp orderResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Status != "filled" {
		t.Fatalf("status: %s", resp.Status)
	}
	if !resp.FilledQty.Equal(decimal.NewFromFloat(0.5)) {
		t.Fatalf("filled qty: %v", resp.FilledQty)
	}
	if resp.Venue != "dummy" {
		t.Fatalf("venue: %s", resp.Venue)
	}
	if resp.Pair != "BTCUSDT" {
		t.Fatalf("pair: %s", resp.Pair)
	}
	if sink.Count() != 2 {
		t.Fatalf("expected 2 audit events (order+fill), got %d", sink.Count())
	}
}

func TestPlaceOrderInvalidJSON(t *testing.T) {
	svc, _, _ := newTestService(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/orders", bytes.NewBufferString("{bad json"))
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPlaceOrderMissingFields(t *testing.T) {
	svc, _, _ := newTestService(t)
	cases := []string{
		`{"pair":"BTCUSDT","side":"buy","type":"market","amount":"0.5"}`,
		`{"venue":"dummy","side":"buy","type":"market","amount":"0.5"}`,
		`{"venue":"dummy","pair":"BTCUSDT","type":"market","amount":"0.5"}`,
		`{"venue":"dummy","pair":"BTCUSDT","side":"buy","amount":"0.5"}`,
		`{"venue":"dummy","pair":"BTCUSDT","side":"buy","type":"market","amount":"0"}`,
		`{"venue":"dummy","pair":"BTCUSDT","side":"buy","type":"market","amount":"notanum"}`,
	}
	for i, b := range cases {
		req := httptest.NewRequest(http.MethodPost, "/v1/orders", bytes.NewBufferString(b))
		rec := httptest.NewRecorder()
		svc.Routes().ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("case %d: expected 400, got %d body=%s", i, rec.Code, rec.Body.String())
		}
	}
}

func TestPlaceOrderWrongMethod(t *testing.T) {
	svc, _, _ := newTestService(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/orders", nil)
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestCancelOrder(t *testing.T) {
	svc, conn, sink := newTestService(t)
	resp, _ := conn.PlaceOrder(context.Background(), venue.OrderRequest{
		ClientOrderID: "c1", Symbol: "BTCUSDT", Side: venue.SideBuy, Type: venue.OrderTypeMarket, Quantity: decimal.NewFromInt(1),
	})
	before := sink.Count()
	req := httptest.NewRequest(http.MethodPost, "/v1/orders/"+resp.VenueOrderID+"/cancel", nil)
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if sink.Count() != before+1 {
		t.Fatalf("expected 1 audit event, got %d", sink.Count()-before)
	}
}

func TestCancelOrderWrongMethod(t *testing.T) {
	svc, _, _ := newTestService(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/orders/x/cancel", nil)
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestGetFills(t *testing.T) {
	svc, conn, _ := newTestService(t)
	resp, _ := conn.PlaceOrder(context.Background(), venue.OrderRequest{
		ClientOrderID: "c2", Symbol: "BTCUSDT", Side: venue.SideBuy, Type: venue.OrderTypeMarket, Quantity: decimal.NewFromInt(1),
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/orders/"+resp.VenueOrderID+"/fills", nil)
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var body struct {
		OrderID string        `json:"order_id"`
		Fills   []venue.Fill `json:"fills"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.OrderID != resp.VenueOrderID {
		t.Fatalf("order id: %s", body.OrderID)
	}
	if len(body.Fills) != 1 {
		t.Fatalf("fills: %d", len(body.Fills))
	}
}

func TestGetFillsWithLimit(t *testing.T) {
	svc, conn, _ := newTestService(t)
	resp, _ := conn.PlaceOrder(context.Background(), venue.OrderRequest{
		ClientOrderID: "c3", Symbol: "BTCUSDT", Side: venue.SideBuy, Type: venue.OrderTypeMarket, Quantity: decimal.NewFromInt(1),
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/orders/"+resp.VenueOrderID+"/fills?limit=5", nil)
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestGetFillsWrongMethod(t *testing.T) {
	svc, _, _ := newTestService(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/orders/x/fills", nil)
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestGetOrderStatus(t *testing.T) {
	svc, conn, _ := newTestService(t)
	resp, _ := conn.PlaceOrder(context.Background(), venue.OrderRequest{
		ClientOrderID: "cx", Symbol: "BTCUSDT", Side: venue.SideBuy, Type: venue.OrderTypeMarket, Quantity: decimal.NewFromInt(1),
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/orders/"+resp.VenueOrderID+"/status", nil)
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var o venue.OrderResponse
	if err := json.NewDecoder(rec.Body).Decode(&o); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if o.VenueOrderID != resp.VenueOrderID {
		t.Fatalf("order id: %s", o.VenueOrderID)
	}
}

func TestOrderSubMissingID(t *testing.T) {
	svc, _, _ := newTestService(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/orders/", nil)
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestOrderSubUnknownSub(t *testing.T) {
	svc, _, _ := newTestService(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/orders/abc/unknown", nil)
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestBalances(t *testing.T) {
	svc, _, _ := newTestService(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/balances", nil)
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var b venue.Balances
	if err := json.NewDecoder(rec.Body).Decode(&b); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !b.Assets["BTC"].Free.Equal(decimal.NewFromFloat(1.5)) {
		t.Fatalf("btc free: %v", b.Assets["BTC"].Free)
	}
}

func TestBalancesWrongMethod(t *testing.T) {
	svc, _, _ := newTestService(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/balances", nil)
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestBookMissingPair(t *testing.T) {
	svc, _, _ := newTestService(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/book/", nil)
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestBookUnknownPair(t *testing.T) {
	svc, _, _ := newTestService(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/book/FOOBAR", nil)
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestBookWrongMethod(t *testing.T) {
	svc, _, _ := newTestService(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/book/BTCUSDT", nil)
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestBookSuccessAfterWait(t *testing.T) {
	svc, _, _ := newTestService(t)
	deadline := time.Now().Add(3 * time.Second)
	for {
		req := httptest.NewRequest(http.MethodGet, "/v1/book/BTCUSDT", nil)
		rec := httptest.NewRecorder()
		svc.Routes().ServeHTTP(rec, req)
		if rec.Code == http.StatusOK {
			var top struct {
				Pair string  `json:"pair"`
				Bid  float64 `json:"bid"`
				Ask  float64 `json:"ask"`
			}
			_ = json.NewDecoder(rec.Body).Decode(&top)
			if top.Bid > 0 && top.Ask > 0 {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("never got book; last code=%d body=%s", rec.Code, rec.Body.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestSplitOrderPath(t *testing.T) {
	cases := []struct {
		path    string
		wantID  string
		wantSub string
	}{
		{"/v1/orders/abc/cancel", "abc", "cancel"},
		{"/v1/orders/abc/fills", "abc", "fills"},
		{"/v1/orders/abc/fills/", "abc", "fills"},
		{"/v1/orders/abc", "abc", ""},
		{"/v1/orders/abc/", "abc", ""},
	}
	for _, c := range cases {
		id, sub := splitOrderPath(c.path)
		if id != c.wantID || sub != c.wantSub {
			t.Fatalf("path %q: got id=%q sub=%q, want id=%q sub=%q", c.path, id, sub, c.wantID, c.wantSub)
		}
	}
}

func TestUnknownRoute(t *testing.T) {
	svc, _, _ := newTestService(t)
	req := httptest.NewRequest(http.MethodGet, "/nope", nil)
	rec := httptest.NewRecorder()
	svc.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestIntegrationServerPlaceCancelFills(t *testing.T) {
	svc, _, _ := newTestService(t)
	srv := httptest.NewServer(svc.Routes())
	defer srv.Close()

	body := `{"venue":"dummy","pair":"BTCUSDT","side":"buy","type":"market","amount":"0.25"}`
	resp, err := http.Post(srv.URL+"/v1/orders", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var or orderResponse
	_ = json.NewDecoder(resp.Body).Decode(&or)
	if or.VenueOrderID == "" {
		t.Fatalf("empty venue order id")
	}

	cancelResp, err := http.Post(srv.URL+"/v1/orders/"+or.VenueOrderID+"/cancel", "application/json", nil)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	defer cancelResp.Body.Close()
	if cancelResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 cancel, got %d", cancelResp.StatusCode)
	}

	getResp, err := http.Get(srv.URL + "/v1/orders/" + or.VenueOrderID + "/fills")
	if err != nil {
		t.Fatalf("get fills: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 fills, got %d", getResp.StatusCode)
	}

	balResp, err := http.Get(srv.URL + "/v1/balances")
	if err != nil {
		t.Fatalf("get balances: %v", err)
	}
	defer balResp.Body.Close()
	if balResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 balances, got %d", balResp.StatusCode)
	}

	hResp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	defer hResp.Body.Close()
	if hResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 healthz, got %d", hResp.StatusCode)
	}

	aResp, err := http.Get(srv.URL + "/admin/status")
	if err != nil {
		t.Fatalf("admin status: %v", err)
	}
	defer aResp.Body.Close()
	if aResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 admin, got %d", aResp.StatusCode)
	}
}