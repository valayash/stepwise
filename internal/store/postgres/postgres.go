// Package postgres is the production store.Store: crash-safe, shared by any
// number of worker processes.
//
// Concurrency model:
//   - Workers claim runs with FOR UPDATE SKIP LOCKED, so concurrent claimers
//     never block each other and never get the same run.
//   - A run's `claims` column doubles as its fencing token. Every worker
//     write runs in a transaction that first locks the run row and checks
//     status = 'running' AND claims = token.
//   - Lease times use the database clock (now()), never a worker's clock.
//   - Change notifications are sent with pg_notify inside the transaction,
//     so Postgres delivers them only if the change commits.
//   - Locks are always taken child-before-parent to avoid deadlocks between
//     a sub-agent finishing and its parent being cancelled.
package postgres

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/valayash/stepwise/internal/core"
	"github.com/valayash/stepwise/internal/store"
)

//go:embed migrations/*.sql
var migrations embed.FS

// NotifyChannel is the LISTEN/NOTIFY channel Stepwise uses.
const NotifyChannel = "stepwise_events"

// Listener receives NOTIFY payloads. LISTEN needs a dedicated connection,
// which database/sql cannot express, so the driver adapter provides it.
type Listener interface {
	Listen(ctx context.Context, channel string) (<-chan string, error)
}

// Store is a Postgres-backed store.Store.
type Store struct {
	store.Hub
	db   *sql.DB
	stop context.CancelFunc
}

var _ store.Store = (*Store)(nil)

// New wraps db. If listener is non-nil, change notifications are fanned out
// to Subscribe; otherwise subscribers rely on polling.
func New(db *sql.DB, listener Listener) (*Store, error) {
	s := &Store{db: db}
	if listener != nil {
		ctx, cancel := context.WithCancel(context.Background())
		s.stop = cancel
		ch, err := listener.Listen(ctx, NotifyChannel)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("listen: %w", err)
		}
		go func() {
			for payload := range ch {
				var ev store.Event
				if json.Unmarshal([]byte(payload), &ev) == nil {
					s.Publish(ev)
				}
			}
		}()
	}
	return s, nil
}

// Close stops the listener. The caller owns db.
func (s *Store) Close() error {
	if s.stop != nil {
		s.stop()
	}
	return nil
}

// Migrate applies embedded migrations once, under an advisory lock so
// several processes starting together don't race.
func Migrate(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	const lockID = 7_465_329_013
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, lockID); err != nil {
		return fmt.Errorf("migration lock: %w", err)
	}
	defer conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, lockID) //nolint:errcheck

	if _, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	names, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		version := strings.TrimSuffix(strings.TrimPrefix(name, "migrations/"), ".sql")
		var done bool
		if err := conn.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, version).Scan(&done); err != nil {
			return err
		}
		if done {
			continue
		}
		sqlText, err := migrations.ReadFile(name)
		if err != nil {
			return err
		}
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, string(sqlText)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("migration %s: %w", version, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// ---- scanning ----

const runCols = `id, status, spec, input, output, error, failure_kind, budget_tokens, tokens_used,
	attempts, claims, lease_owner, lease_expires_at, available_at, parent_run_id, parent_step_id,
	webhook_url, metadata, idempotency_key, triage, created_at, updated_at, completed_at`

func prefixed(cols, p string) string {
	parts := strings.Split(cols, ",")
	for i, c := range parts {
		parts[i] = p + "." + strings.TrimSpace(c)
	}
	return strings.Join(parts, ", ")
}

type scanner interface{ Scan(dest ...any) error }

// scanAll reads every row of a query with scan.
func scanAll[T any](rows *sql.Rows, err error, scan func(scanner) (T, error)) ([]T, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []T
	for rows.Next() {
		v, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func scanRun(sc scanner) (core.Run, error) {
	var (
		r                   core.Run
		spec, meta, triage  []byte
		leaseExp, completed sql.NullTime
		parentRun, idem     sql.NullString
		parentStep          sql.NullInt64
		status, failureKind string
		claims              int
	)
	err := sc.Scan(&r.ID, &status, &spec, &r.Input, &r.Output, &r.Error, &failureKind, &r.BudgetTokens,
		&r.TokensUsed, &r.Attempts, &claims, &r.LeaseOwner, &leaseExp, &r.AvailableAt, &parentRun,
		&parentStep, &r.WebhookURL, &meta, &idem, &triage, &r.CreatedAt, &r.UpdatedAt, &completed)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return r, core.ErrNotFound
		}
		return r, err
	}
	r.Status, r.FailureKind = core.RunStatus(status), core.FailureKind(failureKind)
	r.Claims, r.LeaseToken = claims, int64(claims)
	if err := json.Unmarshal(spec, &r.Spec); err != nil {
		return r, fmt.Errorf("decode spec: %w", err)
	}
	if leaseExp.Valid {
		t := leaseExp.Time
		r.LeaseExpiresAt = &t
	}
	if completed.Valid {
		t := completed.Time
		r.CompletedAt = &t
	}
	r.ParentRunID, r.ParentStepID, r.IdempotencyKey = parentRun.String, parentStep.Int64, idem.String
	if len(meta) > 0 {
		r.Metadata = meta
	}
	if len(triage) > 0 {
		r.Triage = triage
	}
	return r, nil
}

const stepCols = `id, run_id, seq, kind, status, attempts, tool_call_id, tool_name, tool_input, llm,
	result, is_error, approval, child_run_id, duration_ms, created_at, completed_at`

func scanStep(sc scanner) (core.Step, error) {
	var (
		s                    core.Step
		input, llm, approval []byte
		child                sql.NullString
		completed            sql.NullTime
		kind, status         string
	)
	err := sc.Scan(&s.ID, &s.RunID, &s.Seq, &kind, &status, &s.Attempts, &s.ToolCallID, &s.ToolName,
		&input, &llm, &s.Result, &s.IsError, &approval, &child, &s.DurationMs, &s.CreatedAt, &completed)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return s, core.ErrNotFound
		}
		return s, err
	}
	s.Kind, s.Status = core.StepKind(kind), core.StepStatus(status)
	if len(input) > 0 {
		s.ToolInput = input
	}
	if len(llm) > 0 {
		if err := json.Unmarshal(llm, &s.LLM); err != nil {
			return s, fmt.Errorf("decode llm: %w", err)
		}
	}
	if len(approval) > 0 {
		if err := json.Unmarshal(approval, &s.Approval); err != nil {
			return s, fmt.Errorf("decode approval: %w", err)
		}
	}
	s.ChildRunID = child.String
	if completed.Valid {
		t := completed.Time
		s.CompletedAt = &t
	}
	return s, nil
}

// jsonArg converts optional JSON into a query argument (NULL when empty).
func jsonArg(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	return string(raw)
}

func jsonOf(v any) any {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil || string(b) == "null" {
		return nil
	}
	return string(b)
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func secs(d time.Duration) float64 { return d.Seconds() }

// ---- transactions ----

type txFn func(tx *sql.Tx) error

func (s *Store) inTx(ctx context.Context, fn txFn) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// lockFenced locks the run row if the fence is valid.
func lockFenced(ctx context.Context, tx *sql.Tx, f core.Fence) (core.Run, error) {
	r, err := scanRun(tx.QueryRowContext(ctx, `SELECT `+runCols+` FROM runs WHERE id = $1 FOR UPDATE`, f.RunID))
	if err != nil {
		return r, err
	}
	if r.Status != core.RunRunning || r.LeaseToken != f.Token {
		return r, core.ErrFenced
	}
	return r, nil
}

func lockRun(ctx context.Context, tx *sql.Tx, id string) (core.Run, error) {
	return scanRun(tx.QueryRowContext(ctx, `SELECT `+runCols+` FROM runs WHERE id = $1 FOR UPDATE`, id))
}

// notify sends events when (and only if) the transaction commits.
func notify(ctx context.Context, tx *sql.Tx, events ...store.Event) error {
	for _, e := range events {
		b, _ := json.Marshal(e)
		if _, err := tx.ExecContext(ctx, `SELECT pg_notify($1, $2)`, NotifyChannel, string(b)); err != nil {
			return err
		}
	}
	return nil
}

// lockStep locks one of a run's steps.
func lockStep(ctx context.Context, tx *sql.Tx, runID string, stepID int64) (core.Step, error) {
	return scanStep(tx.QueryRowContext(ctx, `SELECT `+stepCols+` FROM steps WHERE id = $1 AND run_id = $2 FOR UPDATE`, stepID, runID))
}

// requeue makes a waiting run claimable now.
func requeue(ctx context.Context, tx *sql.Tx, runID string) error {
	_, err := tx.ExecContext(ctx, `UPDATE runs SET status = 'queued', available_at = now() WHERE id = $1`, runID)
	return err
}

func touch(ctx context.Context, tx *sql.Tx, runID string) error {
	_, err := tx.ExecContext(ctx, `UPDATE runs SET updated_at = now() WHERE id = $1`, runID)
	return err
}

func dbNow(ctx context.Context, tx *sql.Tx) (time.Time, error) {
	var t time.Time
	err := tx.QueryRowContext(ctx, `SELECT now()`).Scan(&t)
	return t, err
}

func stepsOf(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, runID string) ([]core.Step, error) {
	rows, err := q.QueryContext(ctx, `SELECT `+stepCols+` FROM steps WHERE run_id = $1 ORDER BY seq`, runID)
	return scanAll(rows, err, scanStep)
}

// outbox writes a webhook delivery in the caller's transaction.
func outbox(ctx context.Context, tx *sql.Tx, r core.Run, event string, step *core.Step) error {
	if r.WebhookURL == "" || event == "" {
		return nil
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return err
	}
	payload := store.WebhookPayload(event, r, step, now)
	_, err = tx.ExecContext(ctx, `INSERT INTO webhook_deliveries (run_id, url, event, payload) VALUES ($1, $2, $3, $4)`,
		r.ID, r.WebhookURL, event, string(payload))
	return err
}

// ---- runs ----

// CreateRun implements store.Store.
func (s *Store) CreateRun(ctx context.Context, nr core.NewRun) (core.Run, bool, error) {
	var (
		out     core.Run
		created bool
	)
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		r, err := insertRun(ctx, tx, nr)
		if errors.Is(err, errDuplicate) {
			out, err = scanRun(tx.QueryRowContext(ctx, `SELECT `+runCols+` FROM runs WHERE idempotency_key = $1`, nr.IdempotencyKey))
			return err
		}
		if err != nil {
			return err
		}
		out, created = r, true
		return notify(ctx, tx, store.Ready(r.ID))
	})
	return out, created, err
}

var errDuplicate = errors.New("duplicate idempotency key")

func insertRun(ctx context.Context, tx *sql.Tx, nr core.NewRun) (core.Run, error) {
	spec, err := json.Marshal(nr.Spec.WithDefaults())
	if err != nil {
		return core.Run{}, err
	}
	var parentStep any
	if nr.ParentStepID != 0 {
		parentStep = nr.ParentStepID
	}
	row := tx.QueryRowContext(ctx, `
		INSERT INTO runs (id, status, spec, input, budget_tokens, parent_run_id, parent_step_id,
		                  webhook_url, metadata, idempotency_key)
		VALUES ($1, 'queued', $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+runCols,
		core.NewID(), string(spec), nr.Input, nr.Budget(), nullStr(nr.ParentRunID), parentStep,
		nr.WebhookURL, jsonArg(nr.Metadata), nullStr(nr.IdempotencyKey))
	r, err := scanRun(row)
	if errors.Is(err, core.ErrNotFound) {
		return r, errDuplicate
	}
	return r, err
}

// GetRun implements store.Store.
func (s *Store) GetRun(ctx context.Context, id string) (core.Run, error) {
	return scanRun(s.db.QueryRowContext(ctx, `SELECT `+runCols+` FROM runs WHERE id = $1`, id))
}

// ListRuns implements store.Store.
func (s *Store) ListRuns(ctx context.Context, f store.ListFilter) ([]core.Run, error) {
	var (
		where []string
		args  []any
	)
	if f.Status != "" {
		args = append(args, string(f.Status))
		where = append(where, fmt.Sprintf("status = $%d", len(args)))
	}
	if f.ParentRunID != "" {
		args = append(args, f.ParentRunID)
		where = append(where, fmt.Sprintf("parent_run_id = $%d", len(args)))
	}
	q := `SELECT ` + runCols + ` FROM runs`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY created_at DESC, id DESC"
	if f.Limit > 0 {
		args = append(args, f.Limit)
		q += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	return scanAll(rows, err, scanRun)
}

// Usage implements store.Store.
func (s *Store) Usage(ctx context.Context, since time.Time) (store.Usage, error) {
	var u store.Usage
	err := s.db.QueryRowContext(ctx, `SELECT count(*), COALESCE(sum(tokens_used), 0) FROM runs
		WHERE parent_run_id IS NULL AND created_at >= $1`, since).Scan(&u.Runs, &u.Tokens)
	return u, err
}

// ListSteps implements store.Store.
func (s *Store) ListSteps(ctx context.Context, runID string) ([]core.Step, error) {
	var exists bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM runs WHERE id = $1)`, runID).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, core.ErrNotFound
	}
	return stepsOf(ctx, s.db, runID)
}

// ---- workers ----

// ClaimRuns implements store.Store.
func (s *Store) ClaimRuns(ctx context.Context, owner string, limit int, lease time.Duration) ([]core.Run, error) {
	rows, err := s.db.QueryContext(ctx, `
		WITH claimable AS (
			SELECT id FROM runs
			WHERE (status = 'queued' AND available_at <= now())
			   OR (status = 'running' AND lease_expires_at < now())
			ORDER BY available_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE runs r
		SET status = 'running', lease_owner = $1, claims = r.claims + 1,
		    lease_expires_at = now() + make_interval(secs => $3), updated_at = now()
		FROM claimable c
		WHERE r.id = c.id
		RETURNING `+prefixed(runCols, "r"), owner, limit, secs(lease))
	return scanAll(rows, err, scanRun)
}

// Heartbeat implements store.Store.
func (s *Store) Heartbeat(ctx context.Context, f core.Fence, lease time.Duration) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE runs SET lease_expires_at = now() + make_interval(secs => $3)
		WHERE id = $1 AND status = 'running' AND claims = $2`, f.RunID, f.Token, secs(lease))
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return nil
	}
	if _, err := s.GetRun(ctx, f.RunID); errors.Is(err, core.ErrNotFound) {
		return core.ErrNotFound
	}
	return core.ErrFenced
}

// BeginStep implements store.Store.
func (s *Store) BeginStep(ctx context.Context, f core.Fence, st core.Step) (core.Step, error) {
	var out core.Step
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		r, err := lockFenced(ctx, tx, f)
		if err != nil {
			return err
		}
		ex, err := scanStep(tx.QueryRowContext(ctx, `SELECT `+stepCols+` FROM steps WHERE run_id = $1 AND seq = $2 FOR UPDATE`, f.RunID, st.Seq))
		switch {
		case err == nil:
			if ex.Status != core.StepRunning && ex.Status != core.StepApproved {
				return fmt.Errorf("%w: step %d is %s", core.ErrConflict, st.Seq, ex.Status)
			}
			out, err = scanStep(tx.QueryRowContext(ctx, `
				UPDATE steps SET status = 'running', attempts = attempts + 1
				WHERE id = $1 RETURNING `+stepCols, ex.ID))
			if err != nil {
				return err
			}
		case errors.Is(err, core.ErrNotFound):
			status := st.Status
			if status == "" {
				status = core.StepRunning
			}
			attempts := 0
			if status == core.StepRunning {
				attempts = 1
			}
			out, err = scanStep(tx.QueryRowContext(ctx, `
				INSERT INTO steps (run_id, seq, kind, status, attempts, tool_call_id, tool_name, tool_input)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
				RETURNING `+stepCols,
				f.RunID, st.Seq, string(st.Kind), string(status), attempts, st.ToolCallID, st.ToolName, jsonArg(st.ToolInput)))
			if err != nil {
				return err
			}
			if status == core.StepPendingApproval {
				if err := outbox(ctx, tx, r, core.EventApprovalRequested, &out); err != nil {
					return err
				}
			}
		default:
			return err
		}
		if err := touch(ctx, tx, f.RunID); err != nil {
			return err
		}
		return notify(ctx, tx, store.Changed(f.RunID))
	})
	return out, err
}

// SaveStep implements store.Store.
func (s *Store) SaveStep(ctx context.Context, f core.Fence, st core.Step) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		r, err := lockFenced(ctx, tx, f)
		if err != nil {
			return err
		}
		var prev string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM steps WHERE id = $1 AND run_id = $2 FOR UPDATE`, st.ID, f.RunID).Scan(&prev); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return core.ErrNotFound
			}
			return err
		}
		newlyDone := st.Status == core.StepCompleted && prev != string(core.StepCompleted)
		saved, err := scanStep(tx.QueryRowContext(ctx, `
			UPDATE steps SET status = $2, llm = $3, result = $4, is_error = $5, approval = $6,
			       child_run_id = $7, duration_ms = $8, tool_input = COALESCE($9, tool_input),
			       completed_at = CASE WHEN $10 THEN now() ELSE completed_at END
			WHERE id = $1 RETURNING `+stepCols,
			st.ID, string(st.Status), jsonOf(st.LLM), st.Result, st.IsError, jsonOf(st.Approval),
			nullStr(st.ChildRunID), st.DurationMs, jsonArg(st.ToolInput), newlyDone))
		if err != nil {
			return err
		}
		if newlyDone {
			if _, err := tx.ExecContext(ctx, `UPDATE runs SET tokens_used = tokens_used + $2, attempts = 0, error = '', updated_at = now() WHERE id = $1`,
				f.RunID, st.Tokens()); err != nil {
				return err
			}
		} else if err := touch(ctx, tx, f.RunID); err != nil {
			return err
		}
		if st.Status == core.StepPendingApproval && prev != string(core.StepPendingApproval) {
			if err := outbox(ctx, tx, r, core.EventApprovalRequested, &saved); err != nil {
				return err
			}
		}
		return notify(ctx, tx, store.Changed(f.RunID))
	})
}

// SpawnChild implements store.Store.
func (s *Store) SpawnChild(ctx context.Context, f core.Fence, parent core.Step, nr core.NewRun) (core.Step, core.Run, error) {
	var (
		outStep core.Step
		child   core.Run
	)
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		if _, err := lockFenced(ctx, tx, f); err != nil {
			return err
		}
		ps, err := lockStep(ctx, tx, f.RunID, parent.ID)
		if err != nil {
			return err
		}
		if ps.ChildRunID != "" { // spawned before a crash: reuse
			outStep = ps
			child, err = scanRun(tx.QueryRowContext(ctx, `SELECT `+runCols+` FROM runs WHERE id = $1`, ps.ChildRunID))
			return err
		}
		nr.ParentRunID, nr.ParentStepID, nr.IdempotencyKey = f.RunID, ps.ID, ""
		child, err = insertRun(ctx, tx, nr)
		if err != nil {
			return err
		}
		outStep, err = scanStep(tx.QueryRowContext(ctx, `
			UPDATE steps SET status = 'waiting_child', child_run_id = $2 WHERE id = $1 RETURNING `+stepCols, ps.ID, child.ID))
		if err != nil {
			return err
		}
		if err := touch(ctx, tx, f.RunID); err != nil {
			return err
		}
		return notify(ctx, tx, store.Ready(child.ID), store.Changed(f.RunID))
	})
	return outStep, child, err
}

// blocking reports whether a run has steps waiting on a human or sub-agent,
// and whether it has approved work ready.
func blocking(ctx context.Context, tx *sql.Tx, runID string) (blocked, work bool, err error) {
	err = tx.QueryRowContext(ctx, `
		SELECT COALESCE(bool_or(status IN ('pending_approval', 'waiting_child')), false),
		       COALESCE(bool_or(status = 'approved'), false)
		FROM steps WHERE run_id = $1`, runID).Scan(&blocked, &work)
	return
}

// Yield implements store.Store.
func (s *Store) Yield(ctx context.Context, f core.Fence, y store.Yield) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		if _, err := lockFenced(ctx, tx, f); err != nil {
			return err
		}
		status := y.Status
		var available any = y.AvailableAt
		switch status {
		case core.RunQueued:
			if y.AvailableAt.IsZero() {
				available = nil
			}
		case core.RunWaiting:
			blocked, work, err := blocking(ctx, tx, f.RunID)
			if err != nil {
				return err
			}
			if !blocked || work {
				status = core.RunQueued
			}
			available = nil
		default:
			return fmt.Errorf("%w: yield status %q", core.ErrInvalid, y.Status)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE runs SET status = $2, available_at = COALESCE($3, now()), attempts = $4, error = $5,
			       lease_owner = '', lease_expires_at = NULL, updated_at = now()
			WHERE id = $1`, f.RunID, string(status), available, y.Attempts, y.Error); err != nil {
			return err
		}
		if status == core.RunQueued {
			return notify(ctx, tx, store.Changed(f.RunID), store.Ready(f.RunID))
		}
		return notify(ctx, tx, store.Changed(f.RunID))
	})
}

// Finish implements store.Store.
func (s *Store) Finish(ctx context.Context, f core.Fence, o store.Outcome) error {
	if !o.Status.Terminal() {
		return fmt.Errorf("%w: finish status %q", core.ErrInvalid, o.Status)
	}
	return s.inTx(ctx, func(tx *sql.Tx) error {
		r, err := lockFenced(ctx, tx, f)
		if err != nil {
			return err
		}
		return finishTx(ctx, tx, r, o)
	})
}

// finishTx ends a locked run, writes its webhook and resolves its parent step.
func finishTx(ctx context.Context, tx *sql.Tx, r core.Run, o store.Outcome) error {
	done, err := scanRun(tx.QueryRowContext(ctx, `
		UPDATE runs SET status = $2, output = $3, error = $4, failure_kind = $5,
		       lease_owner = '', lease_expires_at = NULL, completed_at = now(), updated_at = now()
		WHERE id = $1 RETURNING `+runCols, r.ID, string(o.Status), o.Output, o.Error, string(o.FailureKind)))
	if err != nil {
		return err
	}
	if err := outbox(ctx, tx, done, store.FinishEvent(o.Status), nil); err != nil {
		return err
	}
	if err := notify(ctx, tx, store.Changed(r.ID)); err != nil {
		return err
	}
	if r.ParentRunID == "" {
		return nil
	}

	// Child is locked; now the parent (child-before-parent ordering).
	parent, err := lockRun(ctx, tx, r.ParentRunID)
	if err != nil {
		return err
	}
	result, isErr := store.ChildResult(o)
	res, err := tx.ExecContext(ctx, `
		UPDATE steps SET status = 'completed', result = $3, is_error = $4, completed_at = now(),
		       duration_ms = (EXTRACT(EPOCH FROM now() - created_at) * 1000)::bigint
		WHERE id = $1 AND run_id = $2 AND status = 'waiting_child'`, r.ParentStepID, parent.ID, result, isErr)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil // parent step already resolved (e.g. parent cancelled)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET tokens_used = tokens_used + $2, updated_at = now() WHERE id = $1`,
		parent.ID, done.TokensUsed); err != nil {
		return err
	}
	if parent.Status == core.RunWaiting {
		blocked, _, err := blocking(ctx, tx, parent.ID)
		if err != nil {
			return err
		}
		if !blocked {
			if err := requeue(ctx, tx, parent.ID); err != nil {
				return err
			}
			if err := notify(ctx, tx, store.Ready(parent.ID)); err != nil {
				return err
			}
		}
	}
	return notify(ctx, tx, store.Changed(parent.ID))
}

// ---- humans ----

// DecideApproval implements store.Store.
func (s *Store) DecideApproval(ctx context.Context, runID string, stepID int64, a core.Approval) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		r, err := lockRun(ctx, tx, runID)
		if err != nil {
			return err
		}
		st, err := lockStep(ctx, tx, runID, stepID)
		if err != nil {
			return err
		}
		if st.Status != core.StepPendingApproval || r.Status.Terminal() {
			return fmt.Errorf("%w: step is %s", core.ErrNotRunnable, st.Status)
		}
		if a.DecidedAt, err = dbNow(ctx, tx); err != nil {
			return err
		}
		if a.Approved {
			_, err = tx.ExecContext(ctx, `UPDATE steps SET status = 'approved', approval = $2 WHERE id = $1`, stepID, jsonOf(a))
		} else {
			_, err = tx.ExecContext(ctx, `UPDATE steps SET status = 'completed', approval = $2, is_error = true, result = $3, completed_at = now() WHERE id = $1`,
				stepID, jsonOf(a), store.RejectionResult(a.Note))
		}
		if err != nil {
			return err
		}
		if r.Status == core.RunWaiting {
			blocked, work, err := blocking(ctx, tx, runID)
			if err != nil {
				return err
			}
			if work || !blocked {
				if err := requeue(ctx, tx, runID); err != nil {
					return err
				}
			}
		}
		if err := touch(ctx, tx, runID); err != nil {
			return err
		}
		return notify(ctx, tx, store.Changed(runID), store.Ready(runID))
	})
}

// Cancel implements store.Store. Descendants are locked deepest-first, the
// same child-before-parent order Finish uses.
func (s *Store) Cancel(ctx context.Context, runID string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
			WITH RECURSIVE tree AS (
				SELECT id, 0 AS depth FROM runs WHERE id = $1
				UNION ALL
				SELECT r.id, t.depth + 1 FROM runs r JOIN tree t ON r.parent_run_id = t.id
			)
			SELECT id FROM tree ORDER BY depth DESC`, runID)
		if err != nil {
			return err
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		if len(ids) == 0 {
			return core.ErrNotFound
		}
		locked := map[string]core.Run{}
		for _, id := range ids {
			r, err := lockRun(ctx, tx, id)
			if err != nil {
				return err
			}
			locked[id] = r
		}
		if locked[runID].Status.Terminal() {
			return fmt.Errorf("%w: run is %s", core.ErrNotRunnable, locked[runID].Status)
		}
		for _, id := range ids { // deepest first
			r := locked[id]
			if r.Status.Terminal() {
				continue
			}
			if err := finishTx(ctx, tx, r, store.Outcome{Status: core.RunCancelled, Error: "cancelled"}); err != nil {
				return err
			}
		}
		return nil
	})
}

// Retry implements store.Store.
func (s *Store) Retry(ctx context.Context, runID string) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		r, err := lockRun(ctx, tx, runID)
		if err != nil {
			return err
		}
		if r.Status != core.RunFailed {
			return fmt.Errorf("%w: only failed runs can be retried (run is %s)", core.ErrNotRunnable, r.Status)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE runs SET status = 'queued', available_at = now(), attempts = 0, error = '',
			       failure_kind = '', completed_at = NULL, updated_at = now()
			WHERE id = $1`, runID); err != nil {
			return err
		}
		return notify(ctx, tx, store.Changed(runID), store.Ready(runID))
	})
}

// SetTriage implements store.Store.
func (s *Store) SetTriage(ctx context.Context, runID string, d json.RawMessage) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		r, err := scanRun(tx.QueryRowContext(ctx, `
			UPDATE runs SET triage = $2, updated_at = now() WHERE id = $1 RETURNING `+runCols, runID, jsonArg(d)))
		if err != nil {
			return err
		}
		if err := outbox(ctx, tx, r, core.EventRunTriaged, nil); err != nil {
			return err
		}
		return notify(ctx, tx, store.Changed(runID))
	})
}

// ---- webhook outbox ----

const deliveryCols = `id, run_id, url, event, payload, status, attempts, next_attempt_at, last_error, created_at`

func scanDelivery(sc scanner) (store.Delivery, error) {
	var d store.Delivery
	var payload []byte
	err := sc.Scan(&d.ID, &d.RunID, &d.URL, &d.Event, &payload, &d.Status, &d.Attempts, &d.NextAttemptAt, &d.LastError, &d.CreatedAt)
	d.Payload = payload
	return d, err
}

// ClaimDeliveries implements store.Store.
func (s *Store) ClaimDeliveries(ctx context.Context, limit int, lease time.Duration) ([]store.Delivery, error) {
	rows, err := s.db.QueryContext(ctx, `
		WITH due AS (
			SELECT id FROM webhook_deliveries
			WHERE status = 'pending' AND next_attempt_at <= now()
			  AND (leased_until IS NULL OR leased_until < now())
			ORDER BY next_attempt_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE webhook_deliveries d
		SET attempts = d.attempts + 1, leased_until = now() + make_interval(secs => $2)
		FROM due WHERE d.id = due.id
		RETURNING `+prefixed(deliveryCols, "d"), limit, secs(lease))
	return scanAll(rows, err, scanDelivery)
}

// CompleteDelivery implements store.Store.
func (s *Store) CompleteDelivery(ctx context.Context, id int64) error {
	return s.execOne(ctx, `UPDATE webhook_deliveries SET status = 'delivered', last_error = '', leased_until = NULL WHERE id = $1`, id)
}

// FailDelivery implements store.Store.
func (s *Store) FailDelivery(ctx context.Context, id int64, next time.Time, errMsg string, dead bool) error {
	status := store.DeliveryPending
	if dead {
		status = store.DeliveryDead
	}
	return s.execOne(ctx, `UPDATE webhook_deliveries SET status = $2, next_attempt_at = $3, last_error = $4, leased_until = NULL WHERE id = $1`,
		id, status, next, errMsg)
}

// ListDeliveries implements store.Store.
func (s *Store) ListDeliveries(ctx context.Context, runID string) ([]store.Delivery, error) {
	q, args := `SELECT `+deliveryCols+` FROM webhook_deliveries ORDER BY id`, []any{}
	if runID != "" {
		q, args = `SELECT `+deliveryCols+` FROM webhook_deliveries WHERE run_id = $1 ORDER BY id`, []any{runID}
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	return scanAll(rows, err, scanDelivery)
}

func (s *Store) execOne(ctx context.Context, q string, args ...any) error {
	res, err := s.db.ExecContext(ctx, q, args...)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return core.ErrNotFound
	}
	return nil
}
