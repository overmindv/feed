package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/overmindv/feed/internal/rebuild"
	"github.com/overmindv/feed/internal/repository"
)

const previewRebuildCommand = "preview-feed-rebuild"

func runPreviewRebuild(args []string, output io.Writer) error {
	flags := flag.NewFlagSet(previewRebuildCommand, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	id := flags.String("run", "", "rebuild run identifier")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || !rebuild.ValidRunID(*id) {
		return errors.New("usage: feed preview-feed-rebuild -run ID")
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is required for the Feed preview")
	}
	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(signals, 2*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return errors.New("cannot open Feed database for the preview")
	}
	defer pool.Close()

	report, err := repository.New(pool, false).CompareFeedPreview(ctx, *id)
	if err != nil {
		if errors.Is(err, repository.ErrFeedPreviewSources) || errors.Is(err, repository.ErrFeedPreviewConflict) {
			return err
		}

		return errors.New("feed preview did not finish; check the database and retry with the same run")
	}

	return json.NewEncoder(output).Encode(report)
}
