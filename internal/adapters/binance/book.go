package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ai-crypto-onramp/gateway-exchange/internal/metrics"
	"github.com/ai-crypto-onramp/gateway-exchange/internal/venue"
	"github.com/ai-crypto-onramp/gateway-exchange/internal/ws"
	"github.com/shopspring/decimal"
)

type depthEvent struct {
	Stream string `json:"stream"`
	Data   struct {
		LastUpdateID int64 `json:"lastUpdateId"`
		Bids         [][2]string `json:"bids"`
		Asks         [][2]string `json:"asks"`
	} `json:"data"`
}

type snapshotResp struct {
	LastUpdateID int64 `json:"lastUpdateID"`
	Bids         [][2]string `json:"bids"`
	Asks         [][2]string `json:"asks"`
}

func SubscribeBook(ctx context.Context, wsBase, restBase string, pairs []string) (<-chan venue.BookUpdate, error) {
	if len(pairs) == 0 {
		return nil, fmt.Errorf("binance: no pairs")
	}
	streams := make([]string, 0, len(pairs))
	for _, p := range pairs {
		streams = append(streams, strings.ToLower(p)+"@depth")
	}
	streamPath := strings.Join(streams, "/")
	url := wsBase + "/" + streamPath

	out := make(chan venue.BookUpdate, 64)
	recons := make(map[string]*ws.BookReconstructor, len(pairs))
	for _, p := range pairs {
		recons[strings.ToLower(p)] = ws.NewBookReconstructor("binance", p)
	}

	client := ws.NewClient(url, "binance")
	client.SetHandler(func(msg []byte) {
		var ev depthEvent
		if err := json.Unmarshal(msg, &ev); err != nil {
			return
		}
		pair := streamToPair(ev.Stream)
		r, ok := recons[strings.ToLower(pair)]
		if !ok {
			return
		}
		seq := ev.Data.LastUpdateID
		ok2, gap := r.ApplyDiff(seq, ev.Data.Bids, ev.Data.Asks)
		if gap {
			r.MarkGap()
			if err := refreshSnapshot(ctx, restBase, pair, r); err == nil {
				metrics.WSGapCount.WithLabelValues("binance").Inc()
			}
			return
		}
		if !ok2 {
			return
		}
		emitUpdate(r, pair, out)
	})

	go func() {
		defer close(out)
		for _, p := range pairs {
			r := recons[strings.ToLower(p)]
			if err := refreshSnapshot(ctx, restBase, p, r); err != nil {
				continue
			}
			emitUpdate(r, p, out)
		}
		_ = client.Run(ctx)
	}()

	return out, nil
}

func refreshSnapshot(ctx context.Context, restBase, pair string, r *ws.BookReconstructor) error {
	u := restBase + "/api/v3/depth?symbol=" + strings.ToUpper(pair) + "&limit=100"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("snapshot status %d", resp.StatusCode)
	}
	var sn snapshotResp
	if err := json.Unmarshal(body, &sn); err != nil {
		return err
	}
	r.ApplySnapshot(sn.Bids, sn.Asks)
	return nil
}

func emitUpdate(r *ws.BookReconstructor, pair string, out chan<- venue.BookUpdate) {
	bids, asks := r.TopN(20)
	upd := venue.BookUpdate{
		Pair: strings.ToUpper(pair),
		TS:   time.Now().UTC(),
	}
	for _, lvl := range bids {
		p, _ := decimal.NewFromString(lvl[0])
		s, _ := decimal.NewFromString(lvl[1])
		upd.Bids = append(upd.Bids, venue.BookLevel{Price: p, Size: s})
	}
	for _, lvl := range asks {
		p, _ := decimal.NewFromString(lvl[0])
		s, _ := decimal.NewFromString(lvl[1])
		upd.Asks = append(upd.Asks, venue.BookLevel{Price: p, Size: s})
	}
	if len(upd.Bids) > 0 && len(upd.Asks) > 0 {
		lag := r.Lag()
		metrics.WSBookLagSeconds.WithLabelValues("binance", pair).Set(lag)
		select {
		case out <- upd:
		default:
		}
	}
}

func streamToPair(stream string) string {
	if idx := strings.Index(stream, "@"); idx > 0 {
		return stream[:idx]
	}
	return stream
}