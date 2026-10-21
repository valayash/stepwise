package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis is a fleet-wide Limiter: every worker process shares the same
// buckets, so N processes together stay under one provider limit.
//
// Each Reserve or Settle is one Lua script, which Redis runs atomically:
// read both buckets, refill them for the time elapsed, check, take, write
// back. No two workers can take the same capacity, and the script uses
// Redis's clock (TIME), so workers with drifting clocks still agree.
type Redis struct {
	client redis.UniversalClient
	rules  Rules
	prefix string
}

// script refills and adjusts the request bucket (KEYS[1]) and the token
// bucket (KEYS[2]).
//
//	ARGV[1] op: "reserve" or "settle"
//	ARGV[2] rpm, ARGV[3] tpm (0 = unlimited)
//	ARGV[4] tokens to reserve, or the settle delta (> 0 takes, < 0 refunds)
//
// reserve returns 0 when granted, else milliseconds to wait (nothing taken).
var script = redis.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local op = ARGV[1]
local rpm, tpm, n = tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])

local function load(key, cap)
  local v = redis.call('HMGET', key, 'level', 'ts')
  local level, ts = tonumber(v[1]), tonumber(v[2])
  if level == nil then return cap end
  return math.min(cap, level + (now - ts) * cap / 60)
end
local function save(key, level)
  redis.call('HSET', key, 'level', level, 'ts', now)
  redis.call('EXPIRE', key, 180)
end

if op == 'settle' then
  if tpm > 0 and n ~= 0 then
    save(KEYS[2], math.min(tpm, load(KEYS[2], tpm) - n))
  end
  return 0
end

local wait, req, tok, need = 0, nil, nil, 0
if rpm > 0 then
  req = load(KEYS[1], rpm)
  if req < 1 then wait = math.max(wait, (1 - req) * 60 / rpm) end
end
if tpm > 0 then
  need = math.min(n, tpm)
  tok = load(KEYS[2], tpm)
  if tok < need then wait = math.max(wait, (need - tok) * 60 / tpm) end
end
if wait > 0 then
  return math.ceil(wait * 1000)
end
if req then save(KEYS[1], req - 1) end
if tok then save(KEYS[2], tok - need) end
return 0
`)

// NewRedis connects to url (redis://host:port/db) and checks the connection.
func NewRedis(ctx context.Context, url string, rules Rules) (*Redis, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	c := redis.NewClient(opt)
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.Ping(pctx).Err(); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("connect to redis: %w", err)
	}
	return &Redis{client: c, rules: rules, prefix: "stepwise:rl"}, nil
}

// NewRedisWithClient uses an existing client; prefix namespaces the keys.
func NewRedisWithClient(c redis.UniversalClient, rules Rules, prefix string) *Redis {
	return &Redis{client: c, rules: rules, prefix: prefix}
}

// keys returns the two bucket keys. The {prefix} hash tag keeps both on
// one Redis Cluster slot, which a multi-key script requires.
func (l *Redis) keys(r Rule) []string {
	base := fmt.Sprintf("%s:{%s}", l.prefix, r.Prefix)
	return []string{base + ":req", base + ":tok"}
}

// Reserve implements Limiter.
func (l *Redis) Reserve(ctx context.Context, model string, n int) (time.Duration, error) {
	r, ok := l.rules.Lookup(model)
	if !ok {
		return 0, nil
	}
	ms, err := script.Run(ctx, l.client, l.keys(r), "reserve", r.RPM, r.TPM, n).Int64()
	if err != nil {
		return 0, fmt.Errorf("redis rate limiter: %w", err)
	}
	return time.Duration(ms) * time.Millisecond, nil
}

// Settle implements Limiter.
func (l *Redis) Settle(ctx context.Context, model string, delta int) error {
	r, ok := l.rules.Lookup(model)
	if !ok || r.TPM == 0 || delta == 0 {
		return nil
	}
	if err := script.Run(ctx, l.client, l.keys(r), "settle", r.RPM, r.TPM, delta).Err(); err != nil {
		return fmt.Errorf("redis rate limiter: %w", err)
	}
	return nil
}

// Close releases the connection.
func (l *Redis) Close() error { return l.client.Close() }
