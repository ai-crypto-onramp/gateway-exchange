package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/ai-crypto-onramp/gateway-exchange/internal/store"
	"github.com/ai-crypto-onramp/gateway-exchange/internal/store/migrations"
)

type DB struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, dsn string) (*DB, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	runner := migrations.NewRunner(
		func(c context.Context, q string, args ...any) error {
			_, err := pool.Exec(c, q, args...)
			return err
		},
		func(c context.Context, version string) (bool, error) {
			var exists bool
			err := pool.QueryRow(c, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, version).Scan(&exists)
			return exists, err
		},
	)
	if err := runner.Up(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &DB{pool: pool}, nil
}

func (d *DB) Close() error {
	d.pool.Close()
	return nil
}

func (d *DB) Ping(ctx context.Context) error { return d.pool.Ping(ctx) }

func (d *DB) RecordOrder(o store.Order) (bool, error) {
	ctx := context.Background()
	if o.CreatedAt.IsZero() {
		o.CreatedAt = time.Now().UTC()
	}
	o.UpdatedAt = time.Now().UTC()
	tag, err := d.pool.Exec(ctx, `INSERT INTO orders
	(venue_order_id, client_order_id, venue, pair, side, order_type, status, filled_qty, avg_price, quantity, price, created_at, updated_at)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
	ON CONFLICT (venue, client_order_id) DO NOTHING`,
	o.VenueOrderID, o.ClientOrderID, o.Venue, o.Pair, o.Side, o.OrderType, o.Status,
	o.FilledQty.String(), o.AvgPrice.String(), o.Quantity.String(), o.Price.String(),
	o.CreatedAt, o.UpdatedAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (d *DB) GetOrderByClientID(clientOrderID string) (*store.Order, bool) {
	ctx := context.Background()
	o, err := scanOrder(d.pool.QueryRow(ctx, orderSelectSQL()+` WHERE client_order_id=$1`, clientOrderID))
	if err != nil {
		return nil, false
	}
	return o, true
}

func (d *DB) RecordFill(f store.Fill) (bool, error) {
	ctx := context.Background()
	if f.Timestamp.IsZero() {
		f.Timestamp = time.Now().UTC()
	}
	tag, err := d.pool.Exec(ctx, `INSERT INTO fills
	(venue_order_id, trade_id, venue, pair, price, quantity, fee, fee_asset, ts)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	ON CONFLICT (venue, trade_id) DO NOTHING`,
	f.VenueOrderID, f.TradeID, f.Venue, f.Pair, f.Price.String(), f.Quantity.String(),
	f.Fee.String(), f.FeeAsset, f.Timestamp)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (d *DB) FillsForOrder(venueOrderID string) ([]store.Fill, error) {
	ctx := context.Background()
	rows, err := d.pool.Query(ctx, `SELECT venue_order_id, trade_id, venue, pair, price, quantity, fee, fee_asset, ts FROM fills WHERE venue_order_id=$1 ORDER BY ts ASC`, venueOrderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []store.Fill{}
	for rows.Next() {
		f, err := scanFill(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func orderSelectSQL() string {
	return `SELECT venue_order_id, client_order_id, venue, pair, side, order_type, status, filled_qty, avg_price, quantity, price, created_at, updated_at FROM orders`
}

func scanOrder(row pgx.Row) (*store.Order, error) {
	var o store.Order
	var filledQty, avgPrice, qty, price string
	if err := row.Scan(&o.VenueOrderID, &o.ClientOrderID, &o.Venue, &o.Pair, &o.Side, &o.OrderType, &o.Status,
		&filledQty, &avgPrice, &qty, &price, &o.CreatedAt, &o.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
		return nil, err
	}
	o.FilledQty, _ = decimal.NewFromString(filledQty)
	o.AvgPrice, _ = decimal.NewFromString(avgPrice)
	o.Quantity, _ = decimal.NewFromString(qty)
	o.Price, _ = decimal.NewFromString(price)
	return &o, nil
}

func scanFill(row pgx.Row) (store.Fill, error) {
	var f store.Fill
	var price, qty, fee string
	if err := row.Scan(&f.VenueOrderID, &f.TradeID, &f.Venue, &f.Pair, &price, &qty, &fee, &f.FeeAsset, &f.Timestamp); err != nil {
		return store.Fill{}, err
	}
	f.Price, _ = decimal.NewFromString(price)
	f.Quantity, _ = decimal.NewFromString(qty)
	f.Fee, _ = decimal.NewFromString(fee)
	return f, nil
}

var _ store.Store = (*DB)(nil)