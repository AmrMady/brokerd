package marketdata

import (
	"math/rand/v2"
	"testing"
	"time"
)

// floorCheck fails the test on any price below a cent and counts quotes
// with the bid at a cent.
type floorCheck struct {
	t      *testing.T
	atCent int
}

func (f *floorCheck) HandleTrade(t Trade) {
	if t.Price.LessThan(cents(1)) {
		f.t.Fatalf("trade at %s", t.Price)
	}
}

func (f *floorCheck) HandleQuote(q Quote) {
	if q.Bid.LessThan(cents(1)) {
		f.t.Fatalf("quote with bid %s", q.Bid)
	}
	if q.Bid.Equal(cents(1)) {
		f.atCent++
	}
}

func TestSimulatorPriceFloor(t *testing.T) {
	t.Parallel()
	sim := simulation{
		rng:     rand.New(rand.NewPCG(1, 1)),
		symbols: []simSymbol{{name: "PNNY", mid: 3, spread: 2}},
		start:   time.Unix(0, 0),
		rate:    1,
	}
	f := floorCheck{t: t}
	for n := range int64(10000) {
		sim.event(&f, n)
	}
	if f.atCent == 0 {
		t.Error("the bid never reached a cent, so the floor wasn't tested")
	}
}
