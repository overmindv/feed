package consumer

import (
	"errors"
	"strings"
	"testing"

	"github.com/overmindv/feed/internal/domain"
)

const (
	materialID      = "a3300000-0000-4000-8000-000000000001"
	materialPayload = `{"event_id":"a3300000-0000-4000-8000-000000000002","event_type":"material.published","schema_version":1,"occurred_at":"2026-11-20T12:00:00Z","aggregate_id":"a3300000-0000-4000-8000-000000000001","aggregate_version":5,"revision_id":"a3300000-0000-4000-8000-000000000003","payload":{"tombstone":false,"metadata":{"title":"Графы"}}}`
)

func TestMaterialEnvelope(t *testing.T) {
	t.Parallel()

	message, err := decodeForSource(sourceMaterial, materialID, []byte(materialPayload))
	if err != nil || message.material == nil {
		t.Fatal(err)
	}
	if change := message.material; change.MaterialID != materialID ||
		change.Title != "Опубликован материал: Графы" || change.AggregateVersion != 5 || change.RevisionID == "" {
		t.Fatal(change)
	}
	archive := strings.NewReplacer(materialPublished, materialArchived, `"tombstone":false`, `"tombstone":true`).Replace(materialPayload)
	message, err = decodeForSource(sourceMaterial, materialID, []byte(archive))
	if err != nil || message.material == nil || message.material.RevisionID != "" || message.material.Title != "" {
		t.Fatal("archive must only carry the version", err)
	}
}

func TestMaterialEnvelopeRejectsInvalidAndPrivateActivity(t *testing.T) {
	t.Parallel()

	cases := []struct {
		old, replacement string
		reason           error
	}{
		{`"schema_version":1`, `"schema_version":2`, domain.RejectInvalidMaterial},
		{`"aggregate_version":5`, `"aggregate_version":0`, domain.RejectInvalidMaterial},
		{`"revision_id":"a3300000-0000-4000-8000-000000000003"`, `"revision_id":"../../private"`, domain.RejectInvalidMaterial},
		{`"tombstone":false,`, "", domain.RejectInvalidMaterial},
		{"Графы", strings.Repeat("x", 257), domain.RejectInvalidMaterial},
		{"Графы", `null\u0000byte`, domain.RejectInvalidMaterial},
		{materialPublished, "material.read_confirmed", domain.RejectUnknownType},
		{materialPublished, "review.accepted", domain.RejectUnknownType},
	}
	for _, tc := range cases {
		value := strings.Replace(materialPayload, tc.old, tc.replacement, 1)
		if _, err := decodeForSource(sourceMaterial, materialID, []byte(value)); !errors.Is(err, tc.reason) {
			t.Error(tc.old, err)
		}
	}
	if _, err := decodeForSource(sourceMaterial, "mismatched-key", []byte(materialPayload)); !errors.Is(err, domain.RejectInvalidMaterial) {
		t.Error(err)
	}
	if _, err := decodeForSource(sourceTask, materialID, []byte(materialPayload)); !errors.Is(err, domain.RejectWrongSource) {
		t.Error(err)
	}
}

func TestMaterialModerationEnvelope(t *testing.T) {
	t.Parallel()

	const original = "a3300000-0000-4000-8000-000000000003"
	const previous = "a3300000-0000-4000-8000-000000000004"
	correction := strings.NewReplacer(materialPublished, materialCorrected, `"metadata":`, `"invalidated_revision_id":"`+previous+`","metadata":`).Replace(materialPayload)
	withdrawal := strings.NewReplacer(materialPublished, materialWithdrawn, `"tombstone":false,"metadata":{"title":"Графы"}`,
		`"tombstone":true,"invalidated_revision_id":"`+original+`"`).Replace(materialPayload)
	restored := strings.Replace(materialPayload, materialPublished, materialRestored, 1)

	for _, tc := range []struct {
		body     string
		revision string
	}{
		{correction, original},
		{withdrawal, ""},
		{restored, original},
	} {
		message, err := decodeForSource(sourceMaterial, materialID, []byte(tc.body))
		if err != nil || message.material == nil || message.material.RevisionID != tc.revision {
			t.Error("unexpected projection", message.material, err)
		}
	}
	for _, value := range []string{
		strings.Replace(correction, previous, original, 1),
		strings.Replace(correction, `"invalidated_revision_id":"`+previous+`",`, "", 1),
		strings.Replace(withdrawal, `"invalidated_revision_id":"`+original+`"`, `"invalidated_revision_id":"`+previous+`"`, 1),
		strings.Replace(restored, `"tombstone":false`, `"tombstone":true`, 1),
	} {
		if _, err := decodeForSource(sourceMaterial, materialID, []byte(value)); !errors.Is(err, domain.RejectInvalidMaterial) {
			t.Error("invalid lifecycle change accepted", err)
		}
	}
}
