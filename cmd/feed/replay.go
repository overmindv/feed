package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"os/signal"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/feed/internal/consumer"
	"github.com/overmindv/feed/internal/domain"
	"github.com/overmindv/feed/internal/repository"
)

var topicName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)

func runReplay(args []string, input io.Reader) error {
	position, allowUnverified, err := parseReplayPosition(args)
	if err != nil {
		return err
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}
	payload, err := consumer.ReadReplayPayload(input)
	if err != nil {
		return err
	}

	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(signals, 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return errors.New("cannot open Feed database")
	}
	defer pool.Close()

	err = consumer.Replay(ctx, repository.New(pool), position, payload, allowUnverified)
	var reason domain.RejectionReason
	switch {
	case err == nil, errors.Is(err, repository.ErrAlreadyReplayed):
		return nil
	case errors.Is(err, repository.ErrRejectionNotFound), errors.Is(err, consumer.ErrNoActivity):
		return err
	case errors.Is(err, consumer.ErrIdentityMismatch), errors.Is(err, consumer.ErrIdentityUnverified):
		return err
	case errors.As(err, &reason):
		return reason
	default:
		return errors.New("replay failed before confirmation; check the database and retry")
	}
}

func parseReplayPosition(args []string) (domain.EventPosition, bool, error) {
	flags := flag.NewFlagSet("replay", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	topic := flags.String("topic", "", "Kafka topic")
	partition := flags.Int64("partition", -1, "Kafka partition")
	offset := flags.Int64("offset", -1, "Kafka offset")
	allowUnverified := flags.Bool("allow-unverified", false, "allow replay if no original identifier is available")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 ||
		!topicName.MatchString(*topic) || *partition < 0 || *partition > 1<<31-1 || *offset < 0 {
		return domain.EventPosition{}, false, errors.New("usage: feed replay -topic TOPIC -partition PARTITION -offset OFFSET [-allow-unverified] < corrected.json")
	}

	return domain.EventPosition{Topic: *topic, Partition: int32(*partition), Offset: *offset}, *allowUnverified, nil
}
