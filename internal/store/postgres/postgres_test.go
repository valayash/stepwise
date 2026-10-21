package postgres_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/valayash/stepwise/internal/core"
	"github.com/valayash/stepwise/internal/store"
	"github.com/valayash/stepwise/internal/store/postgres"
	"github.com/valayash/stepwise/internal/store/postgres/pgxdriver"
	"github.com/valayash/stepwise/internal/store/storetest"
)

// testDB opens a fresh, migrated schema for one test, so tests never see
// each other's rows. Set STEPWISE_TEST_DATABASE_URL to run these tests.
func testDB(t *testing.T) (*sql.DB, postgres.Listener, string) {
	t.Helper()
	base := os.Getenv("STEPWISE_TEST_DATABASE_URL")
	if base == "" {
		t.Skip("STEPWISE_TEST_DATABASE_URL not set")
	}
	admin, _, err := pgxdriver.Open(base)
	if err != nil {
		t.Fatal(err)
	}
	var b [6]byte
	_, _ = rand.Read(b[:])
	schema := "t_" + hex.EncodeToString(b[:])
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
		admin.Close()
	})

	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	db, listener, err := pgxdriver.Open(u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := postgres.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return db, listener, schema
}

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store {
		db, listener, _ := testDB(t)
		s, err := postgres.New(db, listener)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		return s
	})
}

func TestMigrateIsIdempotent(t *testing.T) {
	db, _, _ := testDB(t)
	for i := 0; i < 2; i++ {
		if err := postgres.Migrate(context.Background(), db); err != nil {
			t.Fatalf("migrate #%d: %v", i+2, err)
		}
	}
}

// Notifications are sent inside transactions: a committed change wakes
// subscribers, a rolled-back one never does.
func TestNotifyOnCommit(t *testing.T) {
	db, listener, _ := testDB(t)
	s, err := postgres.New(db, listener)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	events, stop := s.Subscribe()
	defer stop()

	r, _, err := s.CreateRun(context.Background(), core.NewRun{Spec: core.AgentSpec{Model: "mock"}, Input: "x"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-events:
		if ev.Type != store.EventRunsReady || ev.RunID != r.ID {
			t.Errorf("event = %+v", ev)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no notification after commit")
	}
}
