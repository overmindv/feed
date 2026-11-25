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

func runPrepareTasks(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("prepare-task-rebuild", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	id := flags.String("run", "", "rebuild run identifier")
	pages := flags.Int("pages", 20, "maximum journal pages for this invocation")
	allowHTTP := flags.Bool("allow-http", false, "allow HTTP on an isolated test network")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || !rebuild.ValidRunID(*id) ||
		*pages < 1 || *pages > rebuild.MaxTaskPages {
		return errors.New("usage: feed prepare-task-rebuild -run ID [-pages 1..200] [-allow-http]")
	}
	if mode := os.Getenv("ENV"); *allowHTTP && mode != "local" && mode != "test" {
		return errors.New("tasks journal HTTP is only allowed with ENV=local or ENV=test")
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}
	journal, err := rebuild.NewTaskJournal(os.Getenv("FEED_TASKS_URL"), os.Getenv("TASKS_ACTIVITY_TOKEN"), *allowHTTP)
	if err != nil {
		return err
	}
	defer journal.CloseIdleConnections()

	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(signals, 2*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return errors.New("cannot open Feed database")
	}
	defer pool.Close()

	checkpoint, err := rebuild.StageTasks(ctx, journal, repository.New(pool, false), *id, *pages)
	if err != nil {
		if errors.Is(err, rebuild.ErrTaskJournalGap) || errors.Is(err, repository.ErrTaskRebuildAdvanced) {
			return err
		}

		return errors.New("tasks rebuild stopped; check the source and database, then resume with the same run")
	}
	_, err = fmt.Fprintf(output, "Tasks rebuild %s: %d/%d, complete=%t\n",
		*id, checkpoint.Through, checkpoint.Watermark, checkpoint.Through == checkpoint.Watermark)

	return err
}
