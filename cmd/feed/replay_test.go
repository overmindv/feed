package main

import (
	"testing"

	"github.com/overmindv/feed/internal/domain"
)

func TestParseReplayPosition(t *testing.T) {
	t.Parallel()

	position, options, err := parseReplayPosition([]string{"-topic", "task.activity.v1", "-partition", "2", "-offset", "9876543210", "-allow-unverified", "-confirm-material-publication"})
	if err != nil || !options.AllowUnverified || !options.ConfirmMaterialPublication ||
		position != (domain.EventPosition{Topic: "task.activity.v1", Partition: 2, Offset: 9876543210}) {
		t.Fatal(position, options, err)
	}
	for _, args := range [][]string{
		{"-topic", "tasks", "-partition", "0"},
		{"-topic", "tasks", "-partition", "2147483648", "-offset", "0"},
		{"-topic", "tasks", "-partition", "0", "-offset", "-1"},
		{"-topic", "tasks\nwrong", "-partition", "0", "-offset", "0"},
		{"-topic", "tasks", "-partition", "0", "-offset", "0", "extra"},
	} {
		if _, _, err := parseReplayPosition(args); err == nil {
			t.Error("accepted bad arguments", args)
		}
	}
}
