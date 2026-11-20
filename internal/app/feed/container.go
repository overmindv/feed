package feed

import (
	"context"
	"errors"

	"github.com/overmindv/feed/internal/config"
	"github.com/overmindv/feed/internal/consumer"
	"github.com/overmindv/feed/internal/domain"
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

	store := repository.New(pool, cfg.MaterialCardsEnabled)
	feedService := service.New(store)
	if metrics := app.Metrics(); metrics != nil {
		if registerErr := metrics.Register(oldestRejectionAge(store)); registerErr != nil {
			return registerErr
		}
	}

	handle := consumer.NewHandler(store, app.Logger(), cfg.CatalogActivityTopic, cfg.TaskActivityTopic, cfg.MaterialActivityTopic)
	subscriber, err := app.NewSubscriber(cfg.ConsumerGroup,
		cfg.ActivityTopics(),
		func(ctx context.Context, key string, value []byte) error {
			position, ok := parker.PositionFromContext(ctx)
			if !ok {
				return errors.New("missing kafka position")
			}

			return handle(ctx, domain.EventPosition(position), key, value)
		})
	if err != nil {
		return err
	}
	app.AddHealthCheck("kafka", parker.HealthCheckFunc(subscriber.Ping))

	httpapi.Register(app.HTTP(), feedService, cfg.GatewayToken, app.Logger())
	app.AddRunnable("kafka-feed", subscriber.Run)

	return nil
}
