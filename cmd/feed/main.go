package main

import (
	"os"

	appfeed "github.com/overmindv/feed/internal/app/feed"
	"github.com/overmindv/parker"
)

// main запускает Feed на каркасе parker: инфраструктура (конфиг, HTTP, postgres+
// миграции, логирование, метрики, graceful shutdown) — внутри parker, здесь
// только бизнес-логика (см. run).
func main() {
	os.Exit(parker.Main(run, parker.WithAppName("feed")))
}

// run открывает PostgreSQL, собирает сервис ленты, регистрирует Kafka-consumer
// активностей и внутренний HTTP API /v1/feed.
func run(app *parker.App) error {
	return appfeed.Build(app)
}
