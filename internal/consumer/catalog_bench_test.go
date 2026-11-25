package consumer

import (
	"encoding/json"
	"testing"
)

func BenchmarkDecodeCatalogActivity(b *testing.B) {
	wire, err := json.Marshal(catalogFixture())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	for b.Loop() {
		if _, decodeErr := decodeEvent(courseID, wire); decodeErr != nil {
			b.Fatal(decodeErr)
		}
	}
}
