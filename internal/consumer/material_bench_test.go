package consumer

import "testing"

func BenchmarkDecodeMaterialPublication(b *testing.B) {
	wire := []byte(materialPayload)
	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	for b.Loop() {
		if _, err := decodeMaterial(materialPublished, materialID, wire); err != nil {
			b.Fatal(err)
		}
	}
}
