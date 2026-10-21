// Package pgxdriver connects the Postgres store to the pgx driver: a
// database/sql pool for queries and a dedicated connection for LISTEN.
package pgxdriver

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/valayash/stepwise/internal/store/postgres"
)

// Open returns a connection pool and a LISTEN/NOTIFY listener for url.
func Open(url string) (*sql.DB, postgres.Listener, error) {
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, nil, fmt.Errorf("parse database url: %w", err)
	}
	db := stdlib.OpenDB(*cfg)
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(8)
	db.SetConnMaxIdleTime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("connect to postgres: %w", err)
	}
	return db, &Listener{cfg: cfg}, nil
}

// Listener holds one dedicated connection in LISTEN mode and reconnects
// with backoff if it drops. Notifications sent while it is reconnecting are
// lost, which is fine: workers and live views also poll.
type Listener struct {
	cfg *pgx.ConnConfig
	Log *slog.Logger
}

// Listen implements postgres.Listener.
func (l *Listener) Listen(ctx context.Context, channel string) (<-chan string, error) {
	log := l.Log
	if log == nil {
		log = slog.Default()
	}
	// Connect once up front so configuration errors surface immediately.
	conn, err := l.connect(ctx, channel)
	if err != nil {
		return nil, err
	}
	out := make(chan string, 256)
	go func() {
		defer close(out)
		backoff := 100 * time.Millisecond
		for ctx.Err() == nil {
			if conn == nil {
				if conn, err = l.connect(ctx, channel); err != nil {
					log.Warn("postgres listener reconnect failed", "err", err, "retry_in", backoff)
					select {
					case <-ctx.Done():
						return
					case <-time.After(backoff):
					}
					backoff = min(backoff*2, 10*time.Second)
					continue
				}
				backoff = 100 * time.Millisecond
			}
			n, err := conn.WaitForNotification(ctx)
			if err != nil {
				_ = conn.Close(context.Background())
				conn = nil
				continue
			}
			select {
			case out <- n.Payload:
			default: // consumers are slow; they poll as a backstop
			}
		}
		if conn != nil {
			_ = conn.Close(context.Background())
		}
	}()
	return out, nil
}

func (l *Listener) connect(ctx context.Context, channel string) (*pgx.Conn, error) {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(cctx, l.cfg)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Exec(cctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		_ = conn.Close(context.Background())
		return nil, err
	}
	return conn, nil
}
