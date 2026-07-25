package kraken

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ai-crypto-onramp/exchange-connector/internal/metrics"
	"github.com/ai-crypto-onramp/exchange-connector/internal/venue"
	"github.com/ai-crypto-onramp/exchange-connector/internal/ws"
	"github.com/shopspring/decimal"
)

func SubscribeBook(ctx context.Context, wsBase string, pairs []string) (<-chan venue.BookUpdate, error) {
	if len(pairs) == 0 {
		return nil, fmt.Errorf("kraken: no pairs")
	}
	out := make(chan venue.BookUpdate, 64)
	recons := make(map[string]*ws.BookReconstructor, len(pairs))
	pairToInt := make(map[string]int, len(pairs))
	for i, p := range pairs {
		recons[p] = ws.NewBookReconstructor("kraken", p)
		pairToInt[p] = i + 1
	}
	intToPair := make(map[int]string, len(pairs))
	for p, i := range pairToInt {
		intToPair[i] = p
	}

	subs := struct {
		Event      string `json:"event"`
		Subscription struct {
			Name string `json:"name"`
			Depth int    `json:"depth"`
		} `json:"subscription"`
		Pairs []string `json:"pair"`
	}{
		Event: "subscribe",
		Subscription: struct {
			Name  string `json:"name"`
			Depth  int    `json:"depth"`
		}{Name: "book", Depth: 100},
		Pairs: pairs,
	}

	client := ws.NewClient(wsBase, "kraken")
	client.SetHandler(func(msg []byte) {
		var arr []json.RawMessage
		if err := json.Unmarshal(msg, &arr); err != nil {
			return
		}
		if len(arr) < 4 {
			return
		}
		var channelID int
		if err := json.Unmarshal(arr[0], &channelID); err != nil {
			return
		}
		pair, ok := intToPair[channelID]
		if !ok {
			return
		}
		r := recons[pair]
		var bidsRaw, asksRaw [][2]string
		_ = json.Unmarshal(arr[1], &bidsRaw)
		_ = json.Unmarshal(arr[2], &asksRaw)
		if len(bidsRaw) > 0 && len(asksRaw) > 0 {
			r.ApplySnapshot(bidsRaw, asksRaw)
		} else if len(bidsRaw) > 0 {
			seq := time.Now().UnixNano()
			_, _ = r.ApplyDiff(seq, bidsRaw, nil)
		} else if len(asksRaw) > 0 {
			seq := time.Now().UnixNano()
			_, _ = r.ApplyDiff(seq, nil, asksRaw)
		}
		emitUpdate(r, pair, out)
	})

	go func() {
		defer close(out)
		if err := client.Connect(ctx); err != nil {
			return
		}
		_ = client.WriteJSON(subs)
		_ = client.Run(ctx)
	}()

	return out, nil
}

func emitUpdate(r *ws.BookReconstructor, pair string, out chan<- venue.BookUpdate) {
	bids, asks := r.TopN(20)
	upd := venue.BookUpdate{
		Pair: pair,
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
		metrics.WSBookLagSeconds.WithLabelValues("kraken", pair).Set(lag)
		select {
		case out <- upd:
		default:
		}
	}
}