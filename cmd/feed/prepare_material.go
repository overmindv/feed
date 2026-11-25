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
	"github.com/overmindv/feed/internal/rebuild"
	"github.com/overmindv/feed/internal/repository"
	"github.com/overmindv/parker"
	"github.com/twmb/franz-go/pkg/kgo"
)

const prepareMaterialCommand = "prepare-material-rebuild"

func runPrepareMaterial(args []string, output io.Writer) error {
	flags := flag.NewFlagSet(prepareMaterialCommand, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	id := flags.String("run", "", "rebuild run identifier")
	pages := flags.Int("pages", 20, "maximum snapshot pages for this invocation")
	allowInsecure := flags.Bool("allow-insecure", false, "allow plaintext gRPC on an isolated test network")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || !rebuild.ValidRunID(*id) ||
		*pages < 1 || *pages > rebuild.MaxJournalPages {
		return fmt.Errorf("usage: feed %s -run ID [-pages 1..%d] [-allow-insecure]", prepareMaterialCommand, rebuild.MaxJournalPages)
	}
	if *allowInsecure && !localRebuildMode() {
		return errors.New("plaintext Content gRPC is only allowed with ENV=local or ENV=test")
	}
	opts, err := parker.Load()
	if err != nil {
		return errors.New("invalid rebuild infrastructure configuration")
	}
	if opts.DatabaseURL == "" {
		return errors.New("DATABASE_URL is required for the Content snapshot")
	}
	topic, err := config.MaterialRebuildTopic()
	if err != nil {
		return err
	}
	snapshot, err := rebuild.NewMaterialSnapshot(os.Getenv("FEED_CONTENT_GRPC_ADDRESS"),
		os.Getenv("CONTENT_GRPC_CATALOG_TOKEN"), os.Getenv("FEED_CONTENT_GRPC_CA_FILE"), *allowInsecure)
	if err != nil {
		return err
	}
	defer func() { _ = snapshot.Close() }()
	kafkaOptions, err := parker.KafkaClientOptions(opts.KafkaSecurity)
	if err != nil {
		return errors.New("invalid rebuild Kafka security configuration")
	}
	kafka, err := kgo.NewClient(append(kafkaOptions,
		kgo.SeedBrokers(opts.KafkaBrokers...), kgo.BrokerMaxReadBytes(4<<20))...)
	if err != nil {
		return errors.New("cannot open rebuild Kafka client")
	}
	defer kafka.Close()
	positioner, err := rebuild.NewKafkaMaterialPositioner(kafka, topic)
	if err != nil {
		return err
	}

	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(signals, 2*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, opts.DatabaseURL)
	if err != nil {
		return errors.New("cannot open Feed database for the Content snapshot")
	}
	defer pool.Close()

	checkpoint, err := rebuild.StageMaterials(ctx, snapshot, positioner, repository.New(pool, false), *id, *pages)
	if err != nil {
		if errors.Is(err, rebuild.ErrMaterialPosition) || errors.Is(err, rebuild.ErrMaterialSnapshot) ||
			errors.Is(err, repository.ErrMaterialRebuildAdvanced) {
			return err
		}

		return errors.New("content rebuild stopped; check the source and database, then resume with the same run")
	}
	_, err = fmt.Fprintf(output, "Content rebuild %s: watermark=%d, snapshot_complete=%t, unresolved=%d\n",
		*id, *checkpoint.Watermark, checkpoint.Complete, checkpoint.Unresolved)

	return err
}
