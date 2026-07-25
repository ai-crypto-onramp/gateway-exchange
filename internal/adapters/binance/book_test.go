package binance

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ai-crypto-onramp/gateway-exchange/internal/venue"
	"github.com/gorilla/websocket"
)

func startBinWSServer(t *testing.T, handler func(conn *websocket.Conn)) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		handler(conn)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func wsURL(srv *httptest.Server) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func TestSubscribeBookSnapshotAndDiff(t *testing.T) {
	restSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// depth snapshot endpoint
		_, _ = w.Write([]byte(`{"lastUpdateID":100,"bids":[["50000","1"]],"asks":[["50001","0.5"]]}`))
	}))
	t.Cleanup(restSrv.Close)

	wsSrv := startBinWSServer(t, func(conn *websocket.Conn) {
		// Send a depth diff event with seq continuing from snapshot (101).
		ev := map[string]interface{}{
			"stream": "btcusdt@depth",
			"data": map[string]interface{}{
				"lastUpdateId": 101,
				"bids":         [][2]string{{"49999", "0.25"}},
				"asks":         [][2]string{{"50002", "0.1"}},
			},
		}
		b, _ := json.Marshal(ev)
		_ = conn.WriteMessage(websocket.TextMessage, b)
		time.Sleep(100 * time.Millisecond)
		_ = conn.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, err := SubscribeBook(ctx, wsURL(wsSrv), restSrv.URL, []string{"BTCUSDT"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	select {
	case upd := <-ch:
		if upd.Pair != "BTCUSDT" {
			t.Fatalf("pair: %s", upd.Pair)
		}
		if len(upd.Bids) == 0 || len(upd.Asks) == 0 {
			t.Fatalf("expected bids/asks: %+v", upd)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("no update received")
	}
}

func TestSubscribeBookBadStream(t *testing.T) {
	restSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"lastUpdateID":1,"bids":[["50000","1"]],"asks":[["50001","1"]]}`))
	}))
	t.Cleanup(restSrv.Close)

	wsSrv := startBinWSServer(t, func(conn *websocket.Conn) {
		// invalid JSON then unknown stream then a valid update.
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`not-json`))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"stream":"unknown@depth","data":{"lastUpdateId":2,"bids":[["1","1"]],"asks":[["2","1"]]}}`))
		valid := `{"stream":"btcusdt@depth","data":{"lastUpdateId":2,"bids":[["50000","1"]],"asks":[["50001","1"]]}}`
		_ = conn.WriteMessage(websocket.TextMessage, []byte(valid))
		time.Sleep(100 * time.Millisecond)
		_ = conn.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, err := SubscribeBook(ctx, wsURL(wsSrv), restSrv.URL, []string{"BTCUSDT"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("no update received (bad messages should not crash handler)")
	}
}

func TestSubscribeBookGapTriggersSnapshot(t *testing.T) {
	snapCount := 0
	restSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		snapCount++
		_, _ = w.Write([]byte(`{"lastUpdateID":500,"bids":[["50000","1"]],"asks":[["50001","0.5"]]}`))
	}))
	t.Cleanup(restSrv.Close)

	wsSrv := startBinWSServer(t, func(conn *websocket.Conn) {
		// Give the initial snapshot goroutine time to apply the snapshot.
		time.Sleep(200 * time.Millisecond)
		// First diff at seq=101 (no gap) to establish lastSeq.
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"stream":"btcusdt@depth","data":{"lastUpdateId":101,"bids":[["49999","0.25"]],"asks":[]}}`))
		// Second diff jumps to seq=105, triggering a gap (101 -> 105).
		ev := `{"stream":"btcusdt@depth","data":{"lastUpdateId":105,"bids":[["49998","1"]],"asks":[["50002","0.5"]]}}`
		_ = conn.WriteMessage(websocket.TextMessage, []byte(ev))
		time.Sleep(300 * time.Millisecond)
		_ = conn.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, err := SubscribeBook(ctx, wsURL(wsSrv), restSrv.URL, []string{"BTCUSDT"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	// Drain the channel; the snapshot refresh should have happened.
	for {
		select {
		case <-ch:
		case <-ctx.Done():
			if snapCount < 2 {
				t.Fatalf("expected snapshot refresh on gap, snapCount=%d", snapCount)
			}
			return
		}
	}
}

func TestSubscribeBookSnapshotError(t *testing.T) {
	restSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	t.Cleanup(restSrv.Close)

	wsSrv := startBinWSServer(t, func(conn *websocket.Conn) {
		time.Sleep(100 * time.Millisecond)
		_ = conn.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	ch, err := SubscribeBook(ctx, wsURL(wsSrv), restSrv.URL, []string{"BTCUSDT"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	// Channel should close without emitting (snapshot failed).
	for range ch {
	}
}

func TestConnectorSubscribeBookDelegates(t *testing.T) {
	c := NewConnector("k", "s", nil)
	// Force a quick error path by passing empty pairs through the connector method.
	_, err := c.SubscribeBook(context.Background(), nil)
	if err == nil {
		t.Fatalf("expected error for no pairs")
	}
}

func TestConnectorConfigWSDefault(t *testing.T) {
	c := NewConnector("k", "s", nil)
	if c.Config().WSBase == "" {
		t.Fatalf("ws base should default")
	}
	_ = venue.VenueConfig{}
}