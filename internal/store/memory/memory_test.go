package memory

import (
	"testing"

	"github.com/valayash/stepwise/internal/store"
	"github.com/valayash/stepwise/internal/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(*testing.T) store.Store { return New(nil) })
}
