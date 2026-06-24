# Kafka-топики

## Команды

`station.commands.raw` — входящие команды от терминалов.

`station.commands.filtered` — команды после фильтрации.

`station.commands.validated` — команды после проверки структуры.

`station.commands.authorized` — команды после проверки прав.

`station.commands.rejected` — отклонённые команды.

## Шлюзы

`station.gateway.commands` — команды управления шлюзами.

`station.gateway.events` — события шлюзовой системы.

## Доступ

`station.access.commands` — команды управления доступом.

`station.access.events` — события доступа.

## Стыковка

`station.docking.commands` — команды стыковки.

`station.docking.events` — события стыковки.

## Телеметрия

`station.compartment.telemetry` — телеметрия отсеков.

`station.sensor.telemetry` — показания датчиков.

`station.equipment.telemetry` — состояние оборудования.

## Мониторинг и безопасность

`station.monitoring.events` — события мониторинга.

`station.security.events` — события безопасности.

`station.emergency.events` — аварийные события.

`station.safe_mode.commands` — команды перехода в безопасный режим.

## Хранилище

`station.state.write` — запросы на изменение состояния.

`station.state.events` — события изменения состояния.

`station.journal.events` — события для записи в журнал.