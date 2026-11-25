package rebuild

import (
	"testing"
	"time"
)

func TestCatalogPageBounds(t *testing.T) {
	t.Parallel()

	pinned := int64(120)
	for _, tc := range []struct {
		name               string
		watermark, through int64
		more               bool
		revisions          []int64
		valid              bool
	}{
		{name: "filtered page", watermark: 120, through: 110, more: true, valid: true},
		{name: "filtered revisions", watermark: 120, through: 120, revisions: []int64{21, 120}, valid: true},
		{name: "moved watermark", watermark: 121, through: 120, more: true},
		{name: "stuck cursor", watermark: 120, through: 20, more: true},
		{name: "watermark not reached", watermark: 120, through: 119},
		{name: "order", watermark: 120, through: 120, revisions: []int64{22, 21}},
		{name: "outside window", watermark: 120, through: 119, more: true, revisions: []int64{120}},
		{name: "already staged revision", watermark: 120, through: 120, revisions: []int64{20}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			page := CatalogPage{
				Checkpoint: Checkpoint{Watermark: tc.watermark, Through: tc.through},
				More:       tc.more, Changes: make([]CatalogChange, len(tc.revisions)),
			}
			for i, revision := range tc.revisions {
				page.Changes[i].Revision = revision
			}
			if got := validCatalogPage(page, &pinned, 20); got != tc.valid {
				t.Error("unexpected result for page", page, got)
			}
		})
	}
	if validCatalogPage(CatalogPage{Checkpoint: Checkpoint{Watermark: 101, Through: 101}}, nil, 0) {
		t.Error("accepted more revisions than the requested limit")
	}
}

func TestCatalogEntryRejectsInvalidPublicVisibility(t *testing.T) {
	t.Parallel()

	const ancestor = "university:de130000-0000-0000-0000-000000000003"
	for _, tc := range []struct {
		name   string
		change func(*catalogEntry)
	}{
		{"missing visibility", func(e *catalogEntry) { e.Visibility.Visible = nil }},
		{"missing reset flag", func(e *catalogEntry) { e.Visibility.ScopeReset = nil }},
		{"missing ancestors", func(e *catalogEntry) { e.Visibility.Ancestors = nil }},
		{"duplicate ancestor", func(e *catalogEntry) { e.Visibility.Ancestors = []string{ancestor, ancestor} }},
		{"self ancestor", func(e *catalogEntry) { e.Visibility.Ancestors = []string{e.AggregateType + ":" + e.AggregateID} }},
		{"hidden update", func(e *catalogEntry) { e.Payload.Status = "hidden" }},
		{"unpublished visible entry", func(e *catalogEntry) { e.EventType = "catalog.course.unpublished" }},
		{"null in title", func(e *catalogEntry) { e.Payload.Name = "bad\x00title" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			visible, reset := true, true
			entry := catalogEntry{
				Revision: 3, AggregateVersion: 1, EventType: "catalog.course.created", AggregateType: "course",
				EventID: "de130000-0000-0000-0000-000000000001", AggregateID: "de130000-0000-0000-0000-000000000002",
				OccurredAt: time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC),
			}
			entry.Payload.Name, entry.Payload.Status = "Course", catalogActiveStatus
			entry.Visibility.Visible, entry.Visibility.ScopeReset = &visible, &reset
			entry.Visibility.Ancestors = []string{ancestor}
			if _, ok := entry.change(); !ok {
				t.Fatal("valid fixture was rejected")
			}
			tc.change(&entry)
			if _, ok := entry.change(); ok {
				t.Error("invalid entry was accepted")
			}
		})
	}
}
