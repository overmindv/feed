package feed

import (
	"context"
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type rejectionAgeReader interface {
	OldestRejectionAge(context.Context) (float64, error)
}

func oldestRejectionAge(store rejectionAgeReader) prometheus.GaugeFunc {
	return prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "feed_quarantine_oldest_pending_seconds",
		Help: "Age of the oldest unreplayed feed rejection, or zero when none are pending.",
	}, func() float64 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		age, err := store.OldestRejectionAge(ctx)
		if err != nil {
			return math.NaN()
		}
		return age
	})
}
