package consumer

import (
	"context"
	"errors"
	"io"

	"github.com/overmindv/feed/internal/domain"
	"github.com/overmindv/feed/internal/repository"
)

var (
	ErrNoActivity         = errors.New("event does not affect the feed")
	ErrIdentityMismatch   = errors.New("corrected event does not match the quarantined identity")
	ErrIdentityUnverified = errors.New("no identity available; verify the original record before using -allow-unverified")
)

type replayStore interface {
	ReplayRejection(context.Context, domain.EventPosition, func(context.Context, string, domain.RejectionIdentity, repository.ActivityWriter) error) error
}

func ReadReplayPayload(input io.Reader) ([]byte, error) {
	payload, err := io.ReadAll(io.LimitReader(input, maxEventSize+1))
	if err != nil {
		return nil, errors.New("cannot read corrected event")
	}
	if len(payload) > maxEventSize {
		return nil, domain.RejectTooLarge
	}
	return payload, nil
}

func Replay(ctx context.Context, store replayStore, p domain.EventPosition, payload []byte, allowUnverified bool) error {
	return store.ReplayRejection(ctx, p, func(ctx context.Context, source string, identity domain.RejectionIdentity, writer repository.ActivityWriter) error {
		message, err := decodeForSource(source, "", payload)
		if err != nil {
			return err
		}
		if message.catalog == nil && message.task == nil {
			return ErrNoActivity
		}
		switch {
		case identity.Kind != "" && message.identity(identity.Kind) != identity:
			return ErrIdentityMismatch
		case identity.Kind == "" && !allowUnverified:
			return ErrIdentityUnverified
		}
		return message.apply(ctx, writer)
	})
}
