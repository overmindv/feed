# feed

Сервис ленты активности: read-model событий каталога и задач из Kafka,
внутренний HTTP API `GET /v1/feed`.

## Jet: общая конвенция OVM-341

Jet закреплён в `go.mod` на `v2.15.0`; CLI объявлен через `tool` и запускается
командой `go tool jet`. Это сохраняет версию при `go mod tidy` до переноса SQL.
Перевод repository на сгенерированные таблицы выполняется отдельно в OVM-348.
Модели читаются из live-схемы БД владельца после миграций, коммитятся и не
редактируются вручную. Целевой каталог: `internal/adapter/postgres/generated`.
Сервисный `make jet-generate` и drift gate добавляются при переносе SQL;
в OVM-341 рабочий эталон генерации и `make jet-check` реализованы в Parker.

Порядок генерации и проверки дрейфа: [конвенция Jet](https://github.com/overmindv/docs/blob/main/architecture/jet-generation.md).
