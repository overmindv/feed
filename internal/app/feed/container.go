package feed

import (
	"github.com/overmindv/feed/internal/config"
	"github.com/overmindv/feed/internal/consumer"
	"github.com/overmindv/feed/internal/httpapi"
	"github.com/overmindv/feed/internal/repository"
	"github.com/overmindv/feed/internal/service"
	"github.com/overmindv/parker"
)

// Build выполняет wiring бизнес-зависимостей feed на каркас parker: открывает
// базу, настраивает Kafka-consumer активностей (catalog + task) как фоновый
// Runnable и регистрирует HTTP-роут GET /v1/feed.
func Build(app *parker.App) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	pool, err := app.Postgres() // добавляет health-чек "postgres" в /ready
	if err != nil {
		return err
	}

	store := repository.New(pool)
	feedService := service.New(store)

	// Consumer-group активити: читает оба топика одним подписчиком; источник
	// события определяется по event_type внутри конверта.
	subscriber, err := app.NewSubscriber(cfg.ConsumerGroup,
		[]string{cfg.CatalogActivityTopic, cfg.TaskActivityTopic},
		consumer.NewHandler(store, app.Logger()))
	if err != nil {
		return err
	}
	app.AddHealthCheck("kafka", parker.HealthCheckFunc(subscriber.Ping))

	httpapi.Register(app.HTTP(), feedService, cfg.GatewayToken, app.Logger())
	app.AddRunnable("kafka-feed", subscriber.Run)
	return nil
}
