# Подготовка пересборки ленты из журнала Tasks

После миграции Feed задайте в окружении `DATABASE_URL` базы Feed,
`FEED_TASKS_URL` с базовым URL Tasks и сервисный `TASKS_ACTIVITY_TOKEN`.
Токен не передается аргументом команды. Используйте HTTPS; `-allow-http`
разрешен только в изолированной сети с `ENV=local` или `ENV=test`.

```sh
feed prepare-task-rebuild -run task-rebuild-01 -pages 20
```

Команда сохраняет изменения в `feed_rebuild_task_changes`, а watermark
и последнюю принятую ревизию в `feed_rebuild_task_runs`. Страница журнала
и checkpoint сохраняются вместе. После ошибки или исчерпания лимита
страниц повторите команду с тем же `-run`. Новый ID начинает новую
выгрузку с новым watermark. Разрыв ревизий или изменение watermark
не продвигают checkpoint.

`complete=true` означает, что журнал Tasks загружен до закрепленного
watermark. Это не переключает ленту, не меняет Kafka offsets и не
удаляет существующие записи. Перед переключением проекции нужно
сверить порядок, сохранение ID карточек и снятие с публикации со
снимками остальных источников. ID событий журнала Tasks и старого
Kafka outbox не взаимозаменяемы.

## Журнал Entities

Для того же запуска задайте `FEED_ENTITIES_URL` с базовым URL Entities и
выделенный `ENTITIES_FEED_REPLAY_TOKEN`. Условия для HTTPS и `-allow-http`
те же, что для Tasks.

```sh
feed prepare-catalog-rebuild -run task-rebuild-01 -pages 20
```

Команда сохраняет checkpoint и события в `feed_rebuild_catalog_runs` и
`feed_rebuild_catalog_changes`. Пустая страница может продвинуть checkpoint,
если Entities отфильтровал изменения пререквизитов. Скрытия, удаления и
`scope_reset` сохраняются для пересборки; скрытые названия и предки не пишутся.
При неполной истории команда останавливается. Повторный запуск с тем же ID
читает оставшиеся страницы с закрепленным watermark. `complete=true`
относится только к журналу Entities и не переключает ленту.

## Снимок Content

В окружении Feed задайте `FEED_CONTENT_GRPC_ADDRESS` (хост и порт),
`CONTENT_GRPC_CATALOG_TOKEN`, настройки Kafka и `KAFKA_CONTENT_MATERIAL_TOPIC`.
Для частного CA укажите `FEED_CONTENT_GRPC_CA_FILE`. Флаг `-allow-insecure`
отключает TLS только для изолированной сети с `ENV=local` или `ENV=test`.

```sh
feed prepare-material-rebuild -run task-rebuild-01 -pages 20
```

Для нового запуска команда сохраняет ID топика и последние стабильные
offsets всех разделов до запроса снимка. Kafka consumer group не меняется.
Повторный запуск продолжает с той же позиции и watermark. Страница и курсор
пишутся вместе в `feed_rebuild_material_rows` и `feed_rebuild_material_runs`.

`snapshot_complete=true` означает, что снимок прочитан до конца.
`unresolved` считает видимые записи без исходного времени. Такие записи
сохраняются без даты; подставлять текущее время нельзя. Если Content
восстановил даты после импорта или записанные offsets уже недоступны,
нужен новый запуск. Снимок не переключает ленту: сначала нужно дочитать
события Content после сохраненной позиции и сравнить проекции.
