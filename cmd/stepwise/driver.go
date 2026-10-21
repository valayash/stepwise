package main

import "github.com/valayash/stepwise/internal/store/postgres/pgxdriver"

func init() {
	openPostgres = pgxdriver.Open
}
