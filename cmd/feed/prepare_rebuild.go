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
	"github.com/overmindv/feed/internal/rebuild"
	"github.com/overmindv/feed/internal/repository"
)

const (
	prepareTaskCommand    = "prepare-task-rebuild"
	prepareCatalogCommand = "prepare-catalog-rebuild"
)

func runPrepareJournal(command string, args []string, output io.Writer) error {
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	id := flags.String("run", "", "rebuild run identifier")
	pages := flags.Int("pages", 20, "maximum journal pages for this invocation")
	allowHTTP := flags.Bool("allow-http", false, "allow HTTP on an isolated test network")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || !rebuild.ValidRunID(*id) ||
		*pages < 1 || *pages > rebuild.MaxJournalPages {
		return fmt.Errorf("usage: feed %s -run ID [-pages 1..%d] [-allow-http]", command, rebuild.MaxJournalPages)
	}
	if mode := os.Getenv("ENV"); *allowHTTP && mode != "local" && mode != "test" {
		return errors.New("journal HTTP is only allowed with ENV=local or ENV=test")
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}
	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(signals, 2*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return errors.New("cannot open Feed database")
	}
	defer pool.Close()

	repo := repository.New(pool, false)
	var checkpoint rebuild.Checkpoint
	source := "Tasks"
	if command == prepareCatalogCommand {
		source = "Entities"
		journal, openErr := rebuild.NewCatalogJournal(os.Getenv("FEED_ENTITIES_URL"), os.Getenv("ENTITIES_FEED_REPLAY_TOKEN"), *allowHTTP)
		if openErr != nil {
			return openErr
		}
		defer journal.CloseIdleConnections()
		checkpoint, err = rebuild.StageCatalog(ctx, journal, repo, *id, *pages)
	} else {
		journal, openErr := rebuild.NewTaskJournal(os.Getenv("FEED_TASKS_URL"), os.Getenv("TASKS_ACTIVITY_TOKEN"), *allowHTTP)
		if openErr != nil {
			return openErr
		}
		defer journal.CloseIdleConnections()
		checkpoint, err = rebuild.StageTasks(ctx, journal, repo, *id, *pages)
	}
	if err != nil {
		if errors.Is(err, rebuild.ErrTaskJournalGap) || errors.Is(err, repository.ErrTaskRebuildAdvanced) ||
			errors.Is(err, rebuild.ErrCatalogJournalGap) || errors.Is(err, repository.ErrCatalogRebuildAdvanced) {
			return err
		}

		return fmt.Errorf("%s rebuild stopped; check the source and database, then resume with the same run", source)
	}
	_, err = fmt.Fprintf(output, "%s rebuild %s: %d/%d, complete=%t\n",
		source, *id, checkpoint.Through, checkpoint.Watermark, checkpoint.Through == checkpoint.Watermark)

	return err
}
