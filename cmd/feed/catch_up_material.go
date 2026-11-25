package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/feed/internal/config"
	"github.com/overmindv/feed/internal/consumer"
	"github.com/overmindv/feed/internal/rebuild"
	"github.com/overmindv/feed/internal/repository"
	"github.com/overmindv/parker"
	"github.com/twmb/franz-go/pkg/kgo"
)

const catchUpMaterialCommand = "catch-up-material-rebuild"

func runCatchUpMaterial(args []string, output io.Writer) error {
	flags := flag.NewFlagSet(catchUpMaterialCommand, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	id := flags.String("run", "", "rebuild run identifier")
	batches := flags.Int("batches", 20, "maximum Kafka batches for this invocation")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || !rebuild.ValidRunID(*id) ||
		*batches < 1 || *batches > rebuild.MaxJournalPages {
		return fmt.Errorf("usage: feed %s -run ID [-batches 1..%d]", catchUpMaterialCommand, rebuild.MaxJournalPages)
	}
	opts, err := parker.Load()
	if err != nil || opts.DatabaseURL == "" {
		return errors.New("invalid Kafka or database configuration for Content catch-up")
	}
	topic, err := config.MaterialRebuildTopic()
	if err != nil {
		return err
	}
	kafkaOptions, err := materialKafkaOptions(opts)
	if err != nil {
		return err
	}
	kafka, err := kgo.NewClient(kafkaOptions...)
	if err != nil {
		return errors.New("cannot open Kafka client for Content catch-up")
	}
	defer kafka.Close()
	positioner, err := rebuild.NewKafkaMaterialPositioner(kafka, topic)
	if err != nil {
		return err
	}
	source := rebuild.NewKafkaMaterialReplay(positioner, kafkaOptions, consumer.DecodeMaterialRecord)
	defer source.Close()

	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(signals, 2*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, opts.DatabaseURL)
	if err != nil {
		return errors.New("cannot open Feed database for Content catch-up")
	}
	defer pool.Close()

	state, err := rebuild.CatchUpMaterials(ctx, *id, *batches, source, repository.New(pool, false))
	if err != nil {
		if errors.Is(err, rebuild.ErrMaterialReplaySnapshot) || errors.Is(err, rebuild.ErrMaterialReplayRange) ||
			errors.Is(err, rebuild.ErrMaterialReplayBatch) || errors.Is(err, rebuild.ErrMaterialPosition) ||
			errors.Is(err, repository.ErrMaterialRebuildAdvanced) {
			return err
		}

		return errors.New("content catch-up stopped; check Kafka and Feed database, then resume with the same run")
	}
	_, err = fmt.Fprintf(output, "Content catch-up %s: partitions=%d, caught_up=%t\n", *id, len(state.Target.Partitions), state.CaughtUp())

	return err
}
