package main

import (
	"fmt"
	"os"

	appfeed "github.com/overmindv/feed/internal/app/feed"
	"github.com/overmindv/parker"
)

// main запускает Feed на каркасе parker: инфраструктура (конфиг, HTTP, postgres+
// миграции, логирование, метрики, graceful shutdown) — внутри parker, здесь
// только бизнес-логика (см. run).
func main() {
	if len(os.Args) > 1 && os.Args[1] == prepareMaterialCommand {
		if err := runPrepareMaterial(os.Args[2:], os.Stdout); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		return
	}
	if len(os.Args) > 1 && (os.Args[1] == prepareTaskCommand || os.Args[1] == prepareCatalogCommand) {
		if err := runPrepareJournal(os.Args[1], os.Args[2:], os.Stdout); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		return
	}
	if len(os.Args) > 1 && os.Args[1] == "replay" {
		if err := runReplay(os.Args[2:], os.Stdin); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		return
	}
	os.Exit(parker.Main(run, parker.WithAppName("feed")))
}

// run открывает PostgreSQL, собирает сервис ленты, регистрирует Kafka-consumer
// активностей и внутренний HTTP API /v1/feed.
func run(app *parker.App) error {
	return appfeed.Build(app)
}
