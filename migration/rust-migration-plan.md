# agent-run: подробный план полной миграции на Rust

> **Исторический план.** Миграция завершена: Rust — основная реализация с
> версии 0.12.0, Python заморожен на `archive/python-legacy` (`c58d5a0`), а
> Qwen удалён решением [A22](adr/A22-qwen-removal.md). Будущее время, пути
> `src/agent_run`, `rust/` и исходные оценки ниже сохранены как проектный след,
> а не как текущие инструкции. Текущий статус: [status.md](status.md).

**Технический проект и программа приёмки**  
Версия документа: 1.0 · 15 сентября 2026 года  
Репозиторий: `DKotsyuba/agent-run`  
Базовая версия: `0.11.15`  
Базовый коммит: `c9904f9843ba4a0772bdfa8bac5259f18fad9dc3`

> **Исходный статус на 15 сентября 2026:** план реализации, а не отчёт. Он
> оставлен неизменным ниже, чтобы не приписывать раннему проекту поздние
> результаты.

## Как пользоваться документом

Документ предназначен для разработчика или coding-агента, которому поручается перенос всего продукта. Он фиксирует границы работ, существующие контракты, предлагаемую архитектуру, зависимости, последовательность изменений и доказательства, без которых нельзя объявлять миграцию завершённой.

Разделы 1–4 задают рамки и архитектуру. Разделы 5–16 описывают перенос подсистем. Разделы 17–22 превращают проект в исполнимый план: зависимости, этапы, задания, испытания, переключение и риски. Разделы 23–25 задают решения, итоговую приёмку и источники. Приложения содержат матрицу трассируемости, расширенный backlog и форму передачи работы следующему исполнителю.

Обозначение **[Rxx]** отсылает к исходникам или документации agent-run. **[Wxx]** — к внешней первичной документации. Ссылки собраны в разделе 25. Утверждения о существующем поведении отделены от проектных решений словами «сейчас», «целевое решение», «проверить» и «критерий приёмки».

# 1. Цель, границы и определение полной миграции

## 1.1. Цель

Заменить собственный Python/JavaScript-код agent-run нативной реализацией на Rust, сохранив назначение продукта: локальный управляющий сервис для долговечных запусков coding-агентов, проверяемых результатов, продолжений разговоров, доставки уведомлений и маршрутизации по доступным квотам. Сохранить пользовательские интерфейсы CLI, MCP stdio и Unix-socket JSON-RPC, а также возможность читать накопленное состояние. [R01–R05]

Перенос не должен превращать agent-run в клиент прямых API моделей. Codex, Claude Code, GLM через Claude-совместимый CLI и Qwen остаются внешними движками. Их бинарники, OAuth-процедуры, собственные истории и ограничения не переписываются в рамках проекта. [R03, R09, R13]

## 1.2. Что означает «полная»

Собственный исполняемый код продукта, включая демон, supervisor, адаптеры, hooks, утилиты обслуживания и release-инструменты, находится в Rust. Установленный продукт не требует Python, venv, pip, uv или собственного JavaScript-интерпретируемого слоя для выполнения своей бизнес-логики. SQL, TOML, JSON, Markdown, YAML CI и статические ресурсы остаются в своих форматах: их сохранение не является неполной миграцией.

Наличие у внешнего движка Node.js, Python или другого runtime допустимо: это зависимость самого движка, а не скрытая реализация agent-run. Напротив, Rust-бинарник, запускающий старый `agent_run.cli`, встроенный CPython/PyO3 или переименованный `.cjs` из текущего пакета, полной миграцией не считается.

Миграция считается полной только при функциональном покрытии всех перечисленных далее подсистем. Реализация трёх интерфейсов без delivery, capacity, multi-account, native resume и проверок файлов — это промежуточная версия, а не финальный результат.

## 1.3. Обязательная область работ

| Область | Требуемый результат |
|---|---|
| Пользовательские интерфейсы | Существующие команды CLI, 11 общих tools, служебные методы socket API |
| Жизненный цикл | Durable admission, отделённый supervisor, cancel, steer, reconciliation, наблюдение и wait |
| Состояние | Существующая SQLite v16, путь обновления старых схем, история событий, messages, commands |
| Исполнение | Codex, Claude, GLM, Qwen; генерация конфигурации, assets, auth и policy |
| Продолжения | Native context, однозначная lineage, неизменяемые права и идентичность |
| Результаты | Proof v2, чтение legacy v1, защита от подмены и ограничение объёмов |
| Уведомления | Outbox, claims, retries, evidence, Claude UDS, Desktop relay v1/v2/v3 |
| Квоты | Все текущие источники, topology, forecasts, ranking, account/lane weights, reset credits |
| Эксплуатация | Init, doctor, docs, hooks, launchd, release/rollback, секретобезопасные логи |
| Качество | Дифференциальные, crash, concurrency, security, интеграционные и платформенные тесты |

## 1.4. Что нельзя незаметно включать в перенос

Не добавлять Windows, web UI, TCP API, удалённый multi-user-сервис, собственный scheduler задач, прямые LLM API и новый формат БД. Не возвращать удалённый OpenCode runtime только потому, что сохранился legacy-конфиг. Не вводить автоматические runtime-дедлайны и не менять модель полномочий под видом «улучшения Rust-архитектуры». Новые возможности оформляются отдельными решениями после достижения совместимости. [R06, R09]

## 1.5. Два принципиальных ограничения совместимости

**Desktop relay.** В продукте есть собственный `.cjs`, исполняемый host-supplied signed Node. Простая замена его Rust-процессом ещё не доказывает, что Desktop разрешит те же host capabilities. Это ранняя исследовательская задача и блокирующий критерий, а не пункт, который можно закрыть фактом компиляции. Если native Rust-путь не подтверждён, нельзя одновременно объявить полную языковую миграцию и полный parity этой интеграции. [R15]

**Динамические Python-адаптеры.** Строки вида `agent_run.adapters.codex.adapter:ADAPTER` для встроенных адаптеров можно сохранить как конфигурационные aliases. Произвольный импорт внешнего Python-модуля в pure-Rust runtime таким alias не заменяется. Перед выпуском требуется инвентаризация внешних расширений; каждый реально используемый адаптер должен быть перенесён или заменён явно описанным внешним протоколом.

# 2. Базовая ревизия и надёжность исходных сведений

## 2.1. Зафиксированная база

На момент подготовки плана ветка `main` указывает на `c9904f9843ba4a0772bdfa8bac5259f18fad9dc3`, выпуск `0.11.15`. В предыдущем разборе использовался `c2832f36ce9d089158427dab13d9231bf1f16d4a`, выпуск `0.11.14`. Новая база включает исправление Codex resume после первоначальной настройки project trust. План опирается на новую ревизию. [R01, R12]

Проверены metadata ветки, дерево репозитория, ключевые доменные типы, конфигурационные правила, диспетчер, схема SQLite, миграционный механизм, профили, часть service/adapters, код проверки ответов и snapshot-деревьев, CLI parser, capacity ranking и Desktop relay. Также изучены архитектурные и интеграционные документы. Это карта контрактов и проект переноса, а не утверждение о построчном аудите каждого файла или успешном запуске всей исходной test suite.

Ни исходные live-запуски движков, ни Rust-тесты в рамках подготовки документа не выполнялись. Полный машиночитаемый inventory файлов, тестов и ресурсных зависимостей должен быть получен на этапе P0; нельзя подменять его приблизительным числом файлов или строк из этого текста.

## 2.2. Иерархия источников истины

Для наблюдаемого поведения использовать: зафиксированный исходный код вместе с воспроизводимыми тестами; затем schema и wire fixtures; затем актуальные runtime-contract и API-документы; затем README и комментарии. Даже исходный код не является автоматическим разрешением сохранить дефект безопасности: такие расхождения проходят отдельное решение с тестом старого и нового поведения.

| Наблюдение | Значение для миграции |
|---|---|
| README содержит команды установки старого `0.3.1`, тогда как package metadata показывает `0.11.15` | Версию продукта брать из зафиксированного `pyproject.toml`, а не примеров README |
| В architecture упомянута схема v10, в коде schema/migrations — v16 | Переносить фактическую схему v16 и миграции 002–016 |
| В AGENTS/layout остались упоминания timeout/watchdog | Не восстанавливать устаревшее автоматическое завершение runtime |
| API описывает две bounded lanes, старое описание архитектуры акцентирует один dispatch thread | Сохранить изоляцию control/read, а не воспроизвести устаревшую единственную очередь |
| `default_account` валидируется, но runtime-contract говорит, что он игнорируется | Отсутствие account означает native global, а не выбор первого/default account |
| Canonical role и legacy profile имеют разные правила `write` | Не применять одну формулу `request.write && profile.write` к обоим режимам |
| Требования request и canonical role выбираются разными ветками service | До переноса разрешить семантику дополнительных constraints отдельным ADR |
| Автоматические pre-migration backup-файлы могут удаляться после успешной миграции | Для cutover делать отдельную сохраняемую резервную копию |

Основание: README, AGENTS, `state/migrations.py`, `service.py`, `profiles.py`, `docs/api.md`, `docs/runtime-contract.md`. [R01–R09, R14]

## 2.3. Правило обновления baseline

Новая работа начинается с точного SHA, а не с плавающего `main`. Если Python-ветка развивается параллельно, каждое принятое изменение описывается в `migration/baseline-delta.md`: новый commit, изменённые контракты, затронутые Rust-модули, дополнительные fixtures. До такого разбора обновление исходной ветки не считается автоматически включённым в миграцию.

# 3. Карта существующего проекта

## 3.1. Основной поток управления

```text
CLI / MCP stdio / Unix-socket JSON-RPC
                    |
          единый набор tools / dispatch
                    |
               AgentService
             /      |       \
      config/role  StateStore  capacity/delivery
                    |
          durable admission + READY handoff
                    |
         отдельный supervisor процесса
                    |
     подготовка home/auth -> runtime adapter
                    |
            внешний engine-процесс
                    |
       events / transcript / answer proof
                    |
          terminal state + delivery outbox
```

Диаграмма показывает логические границы, а не обещает, что все CLI-команды проходят одинаковый сетевой маршрут. В частности, административные CLI-операции и broker-owned async start имеют разные требования. Транспортам нельзя самостоятельно реализовывать логику жизненного цикла. [R03–R05, R14]

## 3.2. Карта подсистем и точек переноса

| Существующий путь относительно `src/agent_run` | Ответственность | Целевая область Rust |
|---|---|---|
| `domain.py`, `errors.py` | IDs, запросы, статусы, outcomes, типизированные ошибки | domain |
| `config.py`, `native_settings.py`, `accounts.py`, `paths.py` | Строгий TOML, runtime/account selectors, правила путей | config + platform |
| `profiles.py`, `effective_policy.py`, `role_plan.py` | Роли, grants, evidence, immutable resolved plan | domain + config |
| `service.py`, `preparation.py`, `resume.py` | Admission, orchestration, подготовка, наследование контекста | core/service |
| `dispatch.py` | Tools и транспортно-независимая маршрутизация | core/tools |
| `cli.py`, `doc.py`, `doctor.py` | CLI, operator guide и диагностика | app + core/doctor |
| `api_socket.py`, `broker_client.py`, `mcp.py` | Socket broker/client и MCP proxy | app/transports |
| `launch.py`, `launch_evidence.py`, `supervisor*.py` | Bootstrap, READY, владение и наблюдение за child tree | platform + core/supervisor |
| `process_identity.py`, `lifecycle.py`, `wait.py` | Состояние процессов, жизненный цикл и наблюдение | platform + core/lifecycle |
| `state/db.py`, `store.py`, `schema.sql`, `migrations*` | DB access, constraints, migrations | store |
| `state/start.py`, `resume.py`, `activity.py`, `reconciliation.py` | Транзакции admission, lineage, cursors | store + core |
| `state/delivery.py`, `capacity.py`, `run_stats.py`, `diagnostics.py` | Специализированные запросы и projections | store |
| `verify.py` | Проверка сохранённого ответа и terminal evidence | core/artifacts |
| `adapters/base.py`, `registry.py`, `continuation.py` | Adapter API и capabilities | adapters/api |
| `adapters/home.py`, `snapshot_tree.py`, `snapshots.py` | Managed home, публикация и проверка assets | platform/fs + config/snapshots |
| `adapters/environment.py`, `command_policy.py` | Host environment, команды и deny-only policy | config/environment + adapters |
| `adapters/codex/*` | App-server, permissions, models, plugins, skills, trust | adapters/codex |
| `adapters/claude/*`, `adapters/glm/*` | Headless streams, scoped auth, materialization | adapters/claude + adapters/glm |
| `adapters/qwen/*` | Headless Qwen, sandbox и resume | adapters/qwen |
| `adapters/omniroute.py` | Получение внешних quota observations | adapters/capacity_sources |
| `capacity/collect.py`, `codex_appserver.py` | Сбор ограниченных по времени наблюдений | core/capacity/collector |
| `capacity/history.py`, `forecast.py`, `snapshot.py`, `ranking.py`, `order.py`, `advice.py` | История, прогнозы, topology и порядок маршрутов | core/capacity |
| `delivery/base.py`, `dispatch.py` | Outbox lifecycle, claims, evidence | core/delivery |
| `delivery/claude_uds.py`, `codex_queue.py`, `codex_desktop_relay.py`, `codex_desktop_host.cjs` | Каналы доставки и Desktop bridge | platform/ipc + core/delivery |
| `delivery/completion_notice_contract.*` | Общий безопасный формат уведомления | domain/notice + assets |
| `hooks/*` | Session binding и context receipts | core/hooks + app/hooks |
| `api_launchd.py`, `capacity/launchd.py`, `delivery/launchd.py` | Plist и служебный запуск | app/launchd |
| `operator_guide/*` | Поставляемая документация | assets/operator_guide |
| `scripts/release.py`, `scripts/release_local.py` | Публикация, sealed releases и rollback | xtask/release |

Карта построена по дереву проекта и проверенным контрактам. Мелкие файлы-связки могут объединяться при переносе; ни один источник бизнес-поведения не должен исчезнуть без записи в inventory о его новом владельце. [R01–R18]

## 3.3. Данные, которые нельзя потерять

Агентские записи и lineage; native session IDs и сохранённые histories; команды cancel/steer; события и cursor-семантика; raw transcript references; proofs и manifests; orchestrator binding и receipts; недоставленные уведомления; immutable delivery evidence; quota samples и topology snapshots; run usage. Legacy workflow-таблицы сохраняются даже без актуального product path. [R08, R10, R11, R15, R16]

Файловая система — часть состояния, а не временный cache. Бэкап одной SQLite без agent directories, runtime snapshot assets и необходимых native histories не является полноценным резервным комплектом для продолжения работы.

# 4. Предлагаемая архитектура Rust

## 4.1. Организация workspace

Предлагается один Cargo workspace с шестью библиотечными crates, пользовательским бинарником и `xtask`. Это проектное решение для явных границ; делать отдельный crate для каждого короткого Python-файла не нужно.

```text
agent-run/
  Cargo.toml                    # workspace + единые версии зависимостей
  Cargo.lock                    # реальный lock после resolution и тестов
  rust-toolchain.toml           # фиксированный проверенный toolchain
  crates/
    agent-run-domain/           # чистые типы и переходы
    agent-run-platform/         # Unix, process identity, безопасные файлы
    agent-run-config/           # config, roles, immutable snapshots
    agent-run-store/            # SQLite и транзакции
    agent-run-adapters/         # Codex / Claude / GLM / Qwen
    agent-run-core/             # сервис, supervisor, delivery, capacity
    agent-run/                  # CLI, MCP, socket daemon, internal modes
  xtask/                        # packaging, release, migration utilities
  assets/                       # guides, notice contract, native defaults
  sql/                          # schema.sql + migrations/002..016
  tests/fixtures/               # версии wire/DB/artifact fixtures
  tests/acceptance/              # сквозные сценарии
  migration/                    # inventory, parity, ADR, evidence, status
  .github/workflows/            # Rust CI / release
```

## 4.2. Направление зависимостей

`domain` не знает о Tokio, rusqlite, движках и CLI. `platform` реализует OS-примитивы и не принимает продуктовые решения. `config` использует domain/platform для валидации путей и grants. `store` работает с domain и транзакциями. `adapters` используют domain/config/platform, но не напрямую SQL. `core` координирует эти слои. `app` содержит только транспортную композицию. Обратные зависимости запрещены.

Это сохраняет возможность тестировать admission, scoring и transitions без запуска subprocess и без доступа к настоящим ключам. Не вводить абстрактные repository traits для каждой таблицы ради формальности: mock-порты нужны на границах времени, OS, движков и внешних источников, где они дают воспроизводимость.

## 4.3. Бинарник и внутренние режимы

Пользовательское имя остаётся `agent-run`. Тот же собранный файл может иметь скрытые режимы `__supervisor` и, при необходимости, `__spawn-helper`/`__desktop-relay`. Они не являются публичным API. Их вход — версионированный private protocol с размерными границами и строгой валидацией, а не произвольный JSON «для удобства».

Для durable child запускать абсолютный путь именно текущего immutable release, а не `standalone/current`: переключение symlink не должно подменить версию supervisor в середине admission. Секреты и полная launch environment не передаются через argv и не пишутся в bootstrap-файл. [R03, R13]

## 4.4. Модель concurrency

Tokio обслуживает sockets, bounded channels, external I/O и runtime streams. Для SQLite внутри процесса — выделенные owner-контексты с connection, созданным и закрываемым на своём потоке. Control admission и read work имеют отдельные bounded lanes. Supervisor — отдельный процесс со своим store. Не использовать общий `Arc<Mutex<Connection>>` из произвольных async tasks и не отправлять thread-affine connection в `spawn_blocking` на случайный worker.

`wait` и long-poll `list_agents` не должны держать read lane или SQLite-транзакцию во время ожидания. Реализация делает короткий snapshot-read, освобождает владельца и ожидает изменение revision; commit остаётся единственным основанием наблюдаемого прогресса. In-memory notification — ускорение, но не замена persisted revision. [R14]

## 4.5. Типы и ошибки

Ввести validated newtypes: `AgentId`, `RuntimeName`, `AccountLabel`, `Sha256Digest`, `PositiveFinite`, `NonNegativeFinite`, `AbsoluteDirectory`, `RelativeOwnedPath`, `ProcessBirthProof`. Отдельно различать `GlobalAccount` и `NamedAccount`, UTC epoch и monotonic duration, `RuntimeOutcome` и пользовательское принятие результата.

DTO для wire-совместимости не должен автоматически быть внутренним доменным типом. Например, legacy timeout хранится в DTO и persistence, но не превращается в `tokio::time::timeout` вокруг всего исполнения. `Outcome::Succeeded` создаётся только из проверенных engine evidence и answer proof после подтверждения ухода engine leader и его исходной process group; более широкая descendant cleanup оценивается отдельно.

Ошибки должны быть enum с устойчивым machine code, безопасным display message и внутренним source. Не возвращать «успешный» `Result<Value>` с полем `error`. Не получать наружное сообщение форматированием всего runtime payload или launch environment. [R04, R11]

# 5. Публичные контракты и обратная совместимость

## 5.1. Один реестр tools

Единый `ToolRegistry` должен содержать имя, описание, JSON Schema, классификацию control/read и handler каждого инструмента. MCP discovery, socket `tools` и CLI wiring опираются на этот реестр. Служебные CLI-команды не становятся MCP tools автоматически. Существующий набор общих инструментов содержит ровно 11 имён. [R04, R14]

| Tool | Главный контракт, который надо сохранить |
|---|---|
| `start` | Async durable admission; `agent_id`, `created`, `agent`; идемпотентность request_id |
| `resume` | Новый запуск в исходном native conversation; не повторное открытие старой строки |
| `cancel` | Durable command, а не обещание уже завершённого процесса |
| `steer` | Durable команда только активному движку с соответствующей capability |
| `list_agents` | Точная total, bounded page, revision, long-poll до 60 секунд |
| `answer` | Проверенная metadata и ограниченный inline content; не чтение файла без proof |
| `transcript` | Cursor-page; raw_ref остаётся ссылкой, не автоматически раскрытым содержимым |
| `capacity_order` | Чистый read committed evidence; не новый collection и не auto-launch |
| `doc` | Поставляемый topic либо index; предсказуемая ошибка неизвестной темы |
| `models` | Roster с capabilities и health, а не просто список config.models |
| `limits` | Представление сохранённых наблюдений; не запрос к provider при чтении |

На socket дополнительно сохранить `tools`, `ping` и `wait`. Не добавлять `wait` в общий tool set ради удобства реализации: это изменение публичной поверхности. [R14]

## 5.2. Схемы и значения

Зафиксировать JSON schemas из исходного диспетчера в fixture. Сравнивать обязательные поля, типы, `additionalProperties`, nullable union, enum, uniqueItems, имена и описания. Генерация через schemars допустима только с проверкой фактического совпадения; Rust `Option<T>` и автогенератор не гарантируют исходное различие «ключ отсутствует»/«ключ присутствует со значением null».

Особое внимание: `timeout_seconds` в `start`, его nullable форма в `resume`, опциональная `orchestrator`, типы boolean, массив `required_constraints` без дубликатов, пустые строки, NaN/Infinity, переполнение чисел, NUL в системных путях. Вся внешняя валидация происходит до persistence и любого внешнего действия. Сообщение не должно включать значение ошибочно помещённого в config секрета. [R04, R06, R07]

## 5.3. Стабильные envelopes

Сохранить `StartResult`, `AgentView`, `AgentPage`, `CommandView`, `TranscriptPage`, `AnswerView`, `DeliveryView`, cleanup/policy evidence. Новое поле нельзя объявить сохранённым только потому, что совпало название: должны совпасть тип, nullability, единицы, default и смысл.

Для `AnswerView` отдельно проверить `available`, `path`, `relative_path`, `size_bytes`, `sha256`, `content`, `inline_complete`, `kind`, `media_type`, `proof_version`. Для `AgentView` — lineage, effort, phase, process_state, observed_at и acceptance. `succeeded` означает завершение runtime и наличие проверенного ответа; это не доказательство, что задача реализована правильно или принята человеком. [R04, R10, R11]

`start` возвращает snapshot: очень быстрый отказ подготовки может дать уже terminal agent. Нельзя заставлять response всегда иметь `status=starting`, маскируя реальное committed состояние. [R14]

## 5.4. IDs и курсоры

Сохранить формат `ag-YYYYMMDD-HHMMSS-<10 lowercase hex>`, UTC и проверку календарной даты. Для генерации использовать 5 случайных байт системного CSPRNG; ID никогда не принимается как путь без отдельной валидации. Учитывать, что Python `len(str)` и Rust `String::len()` измеряют разные вещи: протокольные ограничения в символах реализовать через Unicode code points, байтовые — через UTF-8 bytes. [R04]

`events.seq`, `messages.seq`, `revision`, `cursor`, `offset` и `sequence` — разные величины. Не заменять cursor временем, не путать нулевой курсор с первой записью и не пропускать события с одинаковым timestamp. Пустая страница и `complete=true` не равны terminal agent. Сохранить точную semantics `next_cursor`/`next_offset` и ограничение page size.

## 5.5. CLI как отдельный контракт

Инвентаризировать команды: `start`, `resume`, `auth`, `login`, `bind`, `cancel`, `answer`, `steer`, `agents`, `transcript`, `models`, `limits`, `context`, `capacity collect/order/launchd`, `delivery status/cancel/dispatch/launchd`, `hook context/bind`, `init`, `doctor`, `mcp`, `api serve/launchd`, `doc`. Приведённый набор взят из CLI parser baseline. [R05]

Сохранить task из stdin через `-`, `resume --task-file`, взаимное исключение task/task-file и transcript follow/full, repeated `--read-root`, session flags, `start --wait`, default workdir, home override. Не добавлять `--task-file` к start как «исправление симметрии» без отдельного изменения интерфейса.

Clap по умолчанию печатает собственный help и errors. Для production ошибок настроить прежний JSON/exit-code контракт, включая отсутствие ANSI в машинном выводе. Help/version могут быть человекочитаемыми только в соответствии с зафиксированным поведением. Выходы каждого verb и terminal status закрепить golden-тестами; не угадывать exit code по общим соглашениям CLI. [R05, W04]

# 6. Конфигурация, роли, полномочия и окружение

## 6.1. Строгая модель TOML

Сохранить config schema_version=1 и группы `core`, `capacity`, `delivery`, `profiles`, `skills`, `mcp`, `environments`, `runtimes`. Не путать версию TOML-конфига с SQLite user_version и версией пакета. Для всех owner-controlled таблиц использовать строгие DTO с запретом неизвестных полей, но не терять специально допускаемые деревья native_settings. [R06]

Разделить три шага: TOML parsing без раскрытия секретов; shape/semantic validation; resolution путей и ссылок на существующие assets. На каждом шаге формировать безопасную диагностическую location. Нельзя включать весь TOML fragment в ошибку, потому что пользователь мог ошибочно поместить в него credential.

Сохранить finite/positive ограничения на веса, интервалы и legacy timeout; запрет boolean вместо number; ограничения page и context sizes; допустимые names/runtime/account labels; проверки references на MCP/environment. Rust-десериализация помогает с типами, но не заменяет semantic checks.

## 6.2. Пути и имена

Runtime binary должен сохранять lexical absolute path после раскрытия `~`, чтобы версия-managed launcher, Homebrew/nvm symlink и argv0 сохраняли исходную семантику. Homes, directories, auth sources и MCP commands имеют другие правила canonicalization. В Rust нельзя механически вызвать `canonicalize()` для каждого `PathBuf`: оно требует существования и меняет symlink semantics. [R06]

Для ещё не созданного пути реализовать отдельное разрешение существующего родителя и остаточных компонентов. Для работающих owned trees применять descriptor-relative no-follow операции. Проверять path component containment, а не строковый prefix: `/project-other` не находится внутри `/project`.

Account labels, profile names, skill IDs и plugin basenames не должны превращаться в произвольные пути. Отдельные проверки запрещают `..`, absolute paths, NUL и glob-синтаксис у explicit snapshot assets. Не использовать glob expansion для операторского списка файлов.

## 6.3. Legacy profiles и canonical roles

Legacy profile без revision допускает только старые grant-поля. Его `write` сужается пользовательским запросом. Canonical role имеет revision и полный набор полей: write, network, allow_external_read_roots, skills, mcp, required_constraints. Неполная canonical role должна быть отвергнута. Canonical role сама задаёт grants; флаг запроса не превращает её автоматически в legacy profile. [R07, R09]

Read roots нормализуются до минимального antichain: canonical absolute directories, без дубликатов и вложенных roots, уже покрытых родителем. При `allow_external_read_roots=false` наличие внешних roots — ошибка, а не тихое удаление.

Canonical skills/MCP нельзя смешивать с legacy runtime списками. ResolvedRolePlan должен быть credential-free, immutable и содержать content revisions выбранных skills, MCP definitions, полный prompt, grants, auth choice и config revision. В финальном payload не должно быть resolved secret bytes или live environment. [R07]

## 6.4. Обязательное решение о required_constraints

Существующая canonical-ветка service выбирает constraints профиля, тогда как интерфейс допускает request-level required_constraints. До переноса зафиксировать тест, который передаёт request constraint сверх canonical role. Затем принять ADR: сохранить точное baseline поведение либо исправить контракт так, чтобы дополнительные требования запроса не исчезали. [R07]

Целевое безопасное предложение — объединение требований role и request без расширения grants, с отказом при отсутствии enforcement. Это **предлагаемое изменение поведения**, не уже подтверждённая совместимость. Его нельзя включать незаметно: нужны fixture «до/после», release note и отдельный sign-off.

## 6.5. Реальные гарантии изоляции

Сохранить отдельные constraints: отключение web tools; внешняя сеть; loopback TCP; Unix IPC; MCP IPC; filesystem write; filesystem read; plugin immutability. Сохранить уровни enforcement: tool_filter, runtime_enforced, os_enforced, advisory, unsupported. [R07]

Generated HOME, список разрешённых read roots и отсутствие WebFetch не доказывают OS containment. `rustix` также не добавляет sandbox сам по себе. Не повышать enforcement только из-за переписывания на memory-safe язык. Требуемая граница считается обеспеченной только при наличии соответствующей проверенной backend capability. [R09, W05]

Для legacy и canonical launch строить auditable EffectivePolicy до durable admission. Persist неизменяемый snapshot принятой policy. При resume нельзя подменять его текущими, возможно более широкими правами.

## 6.6. Native settings

Отдельно перенести reserved roots для Codex, Claude/GLM и Qwen. Model/auth/provider routing, permissions, sandbox, hook/MCP ownership, command helpers и protocol/output mode нельзя переопределять через native_settings. Разрешённые неизвестные tuning keys не означают автоматическую поддержку любого upstream security-sensitive параметра. [R06, R09]

Разрешить строки, bool, integers, finite floats, arrays и tables. Запретить TOML dates/times, null, non-finite floats и ключи с точками, позволяющими неожиданно менять namespace при обратной генерации. Проверять nested arrays, а не только верхнюю таблицу.

Codex defaults для client context и compaction должны остаться packaged baseline; пользовательские tuning-поля меняют только разрешённые keys. Эти параметры не обещают увеличение server-side context. Изменения применяются к новым preparations, а не задним числом к выполняющимся сессиям. [R03, R09]

## 6.7. Accounts и credential boundaries

Отсутствующий account — отдельный global choice, не alias `default`, `base` или `shared`. Явный label выбирает свою credential lineage. `default_account` остаётся legacy-валидируемым полем, но не меняет выбор native global. Нельзя взять первый configured account «для удобства». [R06, R09]

Для Codex перенести labelled homes и auth bridges с теми же именами/местоположением, а также account-scoped roster и quota probes. Для Claude: unlabelled native запуск сохраняет host HOME и не задаёт CLAUDE_CONFIG_DIR; labelled использует private config state. GLM не должен случайно получить native Claude OAuth или чужой provider token. [R09, R13]

Login/auth запускают native OAuth flow, а не копируют access tokens в TOML. Значения environment credentials существуют только в памяти и при передаче конкретному дочернему процессу. Их нельзя сериализовать в identity, attempt evidence, failure text или trace span. Rust secret wrappers должны исключать Serialize и иметь редактированный Debug.

## 6.8. Окружение разработчика

Сейчас продукт сохраняет ordinary host build environment, PATH, toolchains, SDK variables и locale, исключая неподходящие ambient credentials. Изоляция HOME не должна сломать Cargo/Rustup, Node, Python, Git, SDKROOT, proxy policy и language caches. Экспортированные RUSTUP_HOME/CARGO_HOME имеют приоритет; fallback с исходного host HOME применяется только по текущему контракту. [R09]

Не вызывать `set_var`/`remove_var` глобально в многопоточном демоне ради настройки отдельного child. Строить immutable environment map на запуск и передавать её через Command/env. Не выполнять toolchain-install/probe в admission path. Doctor может проверять required_commands отдельно; runtime-local Rust declaration имеет оговорённый приоритет над named environment. [R06, R09]

# 7. SQLite и миграция накопленного состояния

## 7.1. Стратегия схемы

Первый совместимый Rust-релиз должен читать и писать существующую v16 без изменения её номера, если SQL schema и semantics действительно не меняются. Смена языка не является основанием создавать v17 или удалять legacy columns. Неподдерживаемая более новая schema отвергается до любых записей. [R08]

Поставить в Rust те же `schema.sql` и SQL migrations 002–016. Для пустой базы создать текущую schema целиком. Для старой версии последовательно применить отсутствующие deltas. Не пытаться применить актуальный schema.sql поверх исторической базы с таблицами.

## 7.2. Таблицы и их контракты

| Таблица/группа | Что обязательно сохранить |
|---|---|
| `agents` | Status, request JSON, identity, owner birth, lineage, evidence, timestamps |
| `attempts` | Нумерация на agent, adapter state, lifetime попытки |
| `events` | Monotonic seq, переходы, revision и atomic связь с состоянием |
| `messages` | Порядок, role, name, raw_ref, content и независимый cursor |
| `commands` | Pending/claimed/completed, durably admitted cancel/steer |
| `orchestrator_sessions`, `context_receipts` | Caller identity, binding и дедупликация context |
| `deliveries` | Terminal event identity, claim/lease, retries, ambiguous_result |
| `delivery_attempt_evidence` | Неизменяемая bounded запись каждой owned попытки |
| `capacity_samples`, `capacity_route_snapshots` | История плюс атомарно согласованная topology по scope |
| `run_stats` | Usage source, nullable metrics, attribution по конкретному run |
| `reconciliation_cursors` | Bounded проход активных rows без starvation |
| `workflow_*` | Исторические данные, foreign keys и возможность прочитать backup |

Внутренние Rust-модели могут быть выразительнее SQL-строк, но persisted enum spellings и nullable legacy поля должны остаться читаемыми. Изменение типа timestamp на integer milliseconds без миграции запрещено. [R08]

## 7.3. Границы транзакций

**Admission:** проверить replay namespace и лимиты активных агентов; создать agent/attempt и начальные события согласованно; для resume атомарно закрепить единственного successor. Нельзя проверять лимит отдельным SELECT и вставлять строку вне того же сериализованного admission contract.

**Transition:** проверить допустимый исходный status; изменить row и записать event в одной транзакции. Два конкурирующих terminal writers не должны создавать два результата. Нельзя сначала публиковать event, а затем «позже» обновлять agent.

**Command:** очередь команды — durable до ответа клиенту. Claim и завершение защищены от повторного владения. Повторный cancel не должен убивать чужой PID или создавать бесконечные неисполняемые команды.

**Terminal delivery:** новый outbox row связан с terminal event и bound session. Unbound run не получает фиктивную delivery. Evidence попытки и её verdict записываются в одной owned transaction; потерявший lease worker не завершает чужую попытку.

**Capacity collection:** валидные samples и topology scope публикуются согласованно. Сбой одного account не откатывает успешно committed sibling account. Количество собранных samples в отчёте описывает фактические commits, а не длину входного массива. [R03, R08, R15, R16]

## 7.4. Миграционный runner

Сначала захватить межпроцессный schema/init lock, проверить user_version и обязательные v1 tables, затем подготовить consistent backup через SQLite Backup API. Plain copy одного `.db` при активном WAL недопустим. Каждая delta применяется в собственной `BEGIN IMMEDIATE`, с корректной настройкой foreign_keys/legacy_alter_table и обязательным foreign_key_check перед commit. При ошибке вернуть прежний version и оставить пригодный backup. [R08, W08, W09]

Не переносить Python `executescript` буквально и не разбивать SQL строкой `split(';')`: semicolons могут быть внутри SQL constructs. Использовать SQLite-aware execution и проверить фактические transaction semantics rusqlite. Изменение connection PRAGMAs восстанавливать на всех error paths.

Automatic migration backup и операторский cutover backup — разные сущности. Исходный runner удаляет успешные pre-version snapshots; Rust не должен ошибочно полагаться на их вечное наличие. Политика сохранения операторского комплекта задаётся отдельно. [R08]

## 7.5. Совместимость bytes и JSON

SQLite rows сравниваются логически: значения, indexes, constraints, foreign keys и результаты запросов. Byte-identical database files не являются универсальным критерием между разными SQLite builds. Напротив, JSON и artifact bytes, участвующие в SHA-256/identity, требуют точного сохранения по своему versioned contract.

`request_json` и identity comparisons нужно проверить на отсутствующих ключах, null, bool/integer, float representation и порядке списков. Не пересериализовывать все старые rows при первом открытии базы: это может разрушить replay и snapshot identity без формального изменения schema.

## 7.6. Блокирующие испытания БД

Для каждой исторической версии 1–16 получить fixture с данными, выполнить upgrade и сравнить logical schema с fresh v16. Проверить interruption до/во время/после commit; конкурирующий init; newer/foreign/truncated database; WAL; busy timeout; FK integrity; duplicate request IDs; successor uniqueness; query page boundaries. Не считать нулевую пустую базу достаточным migration fixture.

# 8. Supervisor, запуск и владение процессами

## 8.1. Текущая машина состояний

| Исходное состояние | Допустимые следующие состояния |
|---|---|
| `created` | starting, cancelled, failed, lost |
| `starting` | running, cancelled, failed, lost |
| `running` | succeeded, failed, timed_out, cancelling, lost |
| `cancelling` | cancelled, lost |
| terminal | Нет переходов; продолжение создаёт другую строку |

`timed_out` остаётся совместимым историческим outcome, но наличие enum не разрешает вернуть service-owned runtime timeout. Таблица переносится из доменного контракта и проверяется exhaustive-тестами. [R04]

## 8.2. Durable ownership handoff

Последовательность: синхронно validate/admit `starting`; создать private bounded channel; запустить detached supervisor из immutable release; supervisor фиксирует PID и birth proof в своём store; после durable ownership сообщает READY; только затем выполняет slow auth/materialize/prepare и запускает engine. Start возвращает после ownership handoff, а не после первого engine token. [R03]

READY не может быть просто строкой из stdout произвольного subprocess. Это сообщение внутреннего bootstrap protocol, связанное с agent и конкретной launch attempt. Закрытие pipe, malformed frame, переполнение и partial payload должны иметь отдельные безопасные причины отказа.

Отмена до READY и после READY — разные cases. До передачи ownership launcher должен либо завершить bootstrap с подтверждённой очисткой, либо оставить durable evidence для reconciliation. После READY исчезновение CLI/MCP не отменяет агентский run.

## 8.3. Posix spawn и Rust

Предпочтителен `posix_spawn`-first путь с явными file actions, signal mask/defaults, session/process group и CLOEXEC. Не встраивать произвольную Rust-логику в `pre_exec` closure многопоточного broker: документация std ограничивает безопасные действия после fork. Tokio Child drop также не является доказательством завершения и reap. [R02, W01, W07]

Первый spike должен доказать session creation на Linux и macOS. Возможны audited platform spawn backend либо отдельный однопоточный helper, который после exec создаёт session до инициализации async runtime и продолжает bootstrap. Выбор фиксируется ADR и syscall/process-tree evidence. Не предполагать, что `process_group(0)` эквивалентен `setsid`, и не рассчитывать на nightly-only API без явно выбранного toolchain.

## 8.4. Process identity

PID недостаточен из-за reuse. Сохранить совместимость с историческим birth time, дополнительно выделив источник и точность в platform adapter. Linux-реализация может использовать boot identity + process start ticks для внутренних proofs; Darwin — поддерживаемый process information API. Нельзя изменить persisted формат без доказанной совместимости старого owner observation.

Наблюдение различает alive, dead, reused, unknown и denied. Только dead/reused дают основание маркировать owned run lost. Unknown/denied не превращаются в смерть по возрасту. `kill(pid, 0)` и наличие `/proc/<pid>` сами по себе не подтверждают принадлежность процесса запуску. [R03, R18]

Сигналы разрешены только после положительной проверки ownership. Ошибка прочтения birth proof — fail closed. Короткоживущий engine может исчезнуть до discovery PGID; результат устанавливается по совокупности engine outcome, proof и наблюдений, а не автоматически failed или succeeded.

## 8.5. Cancellation и cleanup

Durable cancel подхватывается supervisor. Сначала использовать native interruption, если принадлежность session/process подтверждена; затем bounded grace и сигналы только подтверждённой собственной process group. TERM/KILL, ожидание завершения и reap — разные действия, их evidence нельзя смешивать.

До сигналов фиксировать readable descendants для последующего наблюдения. В baseline escaped descendant не сигналится индивидуально; этот запрет необходимо сохранить. Переиспользованный PID нельзя «добивать» по старому списку. После cleanup фиксировать scope, signals, group_gone, descendants_gone, confirmed и диагностический PGID. Наличие SIGKILL в журнале не означает `confirmed=true`.

Если исходная process group продолжает жить, success запрещён. Более широкое descendant cleanup evidence в baseline учитывается отдельно от runtime outcome: отсутствие полной containment-гарантии нельзя скрывать, но и нельзя незаметно изменить классификацию всех запусков. Native resume дополнительно проверяет принадлежность и отсутствие мешающих процессов. Нельзя уничтожать весь descendant tree broker: он содержит другие независимые агенты. [R03, R10, R14, R18]

## 8.6. Четыре разных вида времени

| Время/таймаут | Семантика |
|---|---|
| `start.timeout_seconds` | Legacy stored field; не убивает runtime |
| `wait.timeout_seconds` | Ограничивает наблюдателя; agent продолжает исполняться |
| Bootstrap/IPC/response deadlines | Защищают bounded обмен и доступность control path |
| Collector/delivery deadlines и lease | Ограничивают внешнюю пробу или попытку доставки |

Для интервалов внутри процесса применять monotonic clock; persisted observations остаются совместимыми UTC epoch values. Clock jump не должен случайно истечь runtime, а future quota observation не должен становиться свежим. [R03, R14–R16]

## 8.7. Восстановление после падений

Daemon restart не должен перезапускать уже admitted task. Живой supervisor продолжает работать самостоятельно. Reconciliation проходит bounded страницы, сохраняет cursor и не считает отсутствие heartbeat дефектом. После supervisor crash терминальное решение зависит от доказательств процессов; неизвестность не даёт права повторно отправить prompt.

Нужны fault-injection точки после INSERT, после spawn, до/после owner commit, до/после READY, перед engine spawn, после seal answer, между terminal state и внешней delivery. Для каждой точки описать: surviving process, допустимое состояние БД, ожидаемый следующий recovery step, отсутствие duplicate execution и чужих signals.

# 9. Ответы, доказательства и файловые snapshots

## 9.1. Ответ — артефакт с независимым доказательством

Сохранить два формата. Legacy v1 использует точный завершающий frame с `<<<agent-run:complete>>>`. Текущий v2 хранит точные UTF-8 bytes engine payload без дописанного sentinel, файл `.answer-format` со значением `2\n` и соседний `<answer>.proof.json`. Наличие marker/proof исключает downgrade к legacy при повреждении sidecar. [R11]

Proof v2 связывает `proof_version`, `kind`, `media_type`, `answer`, `bytes`, `sha256`. Текст ответа не редактируется, не trim-ится, не нормализует переводы строк и не получает служебную подпись. Сохраняется семантика `agent_answer` и `text/markdown; charset=utf-8`. Нельзя хешировать декодированную, обрезанную или уже преобразованную строку вместо исходных bytes.

## 9.2. Публикация и чтение

Публиковать marker, затем payload, затем proof с обязательной синхронизацией данных и directory entries в предусмотренном порядке. Atomic rename выполняется внутри нужной файловой системы; `flush()` языка и успешная `write()` не равны crash durability. Временные owned files имеют отличимое имя, закрытые permissions и явную recovery-классификацию.

Читать файлы через открытый owned directory descriptor, проходя каждый компонент с no-follow и проверкой regular file. Не ограничиваться canonicalize-then-open: имя может быть подменено между проверкой и чтением. Зафиксировать допустимые auth symlinks отдельно; исключение для credential bridge не распространяется на answer, proof и manifest. [R11]

Сохранить предел проверки payload 16 MiB и отдельный inline threshold 1 MiB. Даже если ответ больше inline threshold и content не возвращается, весь разрешённый payload должен пройти проверку size, SHA-256 и UTF-8. Metadata marker/proof имеют независимый предел 4096 bytes. [R11]

## 9.3. Классификация результата

Верификатор получает engine outcome, stop reason, answer proof и факты process cleanup. Exit code 0 без проверенного завершённого ответа не становится success. Complete proof сам по себе также не отменяет engine error или surviving process group. [R11]

Различать `no_answer`, `answer_incomplete`, `engine_vanished`, `engine_group_survived`, ошибки encoding/tampering/oversize и классифицированный provider failure. Ответ «задачу блокирует отсутствующий доступ» может быть корректным завершённым ответом; не смешивать это с transport error-only response. Решение о принятии реализации остаётся у orchestrator/пользователя. [R10, R11]

## 9.4. Managed trees и immutable config

Сохранить no-follow traversal, список path/type/bytes/hash и executable mode policy. Snapshot manifest публикуется последним. Повторная публикация допускается только согласно исходному контракту проверенного дерева и topology; нельзя «починить» повреждённый snapshot удалением неизвестных файлов или пересчётом доверенного hash. [R11]

Recovery inspection различает owned temps, orphan entries, missing references, type mismatches и hash mismatches. Это диагностика, не автоматическая уборка. Explicit plugin assets сохраняют заявленный scope; отсутствие других files не доказывает, что plugin не обратится к внешнему mutable asset.

## 9.5. Межъязыковая канонизация

Критический риск: JSON-equivalent не означает byte-identical. В исходных Python hash-путях встречаются `sort_keys=True`, компактные separators и стандартное ASCII escaping; обычный `serde_json::to_vec` может выдать другие Unicode/float bytes. Нужно вынести `LegacyCanonicalJsonV1` в отдельный модуль и покрыть golden fixtures. [R07, R11, W11, W12]

Fixture-набор включает кириллицу, emoji вне BMP, управляющие символы, `/`, кавычки, backslash, U+2028/U+2029, пустые коллекции, null, 1/1.0/-0.0, nested arrays и порядок ключей. Не заменять историческую канонизацию JSON Canonicalization Scheme без версии формата и стратегии чтения старых snapshots.

Новые, ещё не подтверждённые формы JSON не записываются в старый snapshot version. Для существующих artifacts безопаснее проверять исходные сохранённые bytes, чем пересобирать их из DTO и объявлять «эквивалентными».

# 10. Адаптер Codex

## 10.1. App-server вместо упрощённого exec

Перенести существующий app-server JSON-RPC драйвер, а не заменить его `codex exec` ради быстрого smoke test. Требуются инициализация, native model roster, session/thread ownership, turn lifecycle, permission requests, steering/interruption, streaming transcripts, limits и resume. Upstream official contract полезен для проверки протокола, но baseline fixtures проекта определяют совместимость текущей интеграции. [R13, W10]

Внутри адаптера разделить wire types, correlated RPC transport, session state machine, stream normalization, permissions, materialization, accounts, model cache и error classification. Unmatched/duplicate responses и unknown notifications должны обрабатываться осознанно; нельзя считать любое JSON с `result` завершением turn.

## 10.2. Поток и завершение turn

Journaling raw assistant deltas идёт по мере поступления. Normalized transcript сохраняет пробелы, повторяющиеся deltas и последний непустой сегмент с прилегающим whitespace. Idle poll и interrupt request не закрывают text stream. Canonical completed item добавляет только текст, который ещё не был сохранён. [R03]

Success возможен только для текущего run/turn. После `thread/resume` старый completed item не удовлетворяет новый completion proof. Unexpected app-server EOF — transport failure, не бесконечная тишина. Явный error.kind/error.code имеет приоритет перед bounded fallback classification; provider_overloaded остаётся отдельным типом, не общим неизвестным error. [R03, R10]

## 10.3. Permissions и generated Projects

Перенести различия read-only, ordinary write и network roles. Workdir обычно ограничивает writable scope; operator-authorized workspace_root допускается только после containment check. Managed `/etc/codex/requirements.toml` Projects нельзя затереть конфликтующим generated профилем. Effective write roots, network и active profile echo должны соответствовать заявленному режиму до первого turn. [R09]

Сохранить разрешённые cache directories для uv/Cargo/npm/pip/Go, запрет shell-доступа к auth bridge и deny-only слой объявленного DCG hook. MCP approval modes остаются исходными; permission hook не должен превращать неизвестные tools или shell calls в blanket approval. Не выдавать Full Access как fallback при ошибке Projects.

## 10.4. Исправление версии 0.11.15

Новый `adapters/codex/snapshot.py` добавляет точный native trust receipt выбранного resolved workdir до окончательного sealing snapshot. Повторная fresh preparation не меняет уже корректные bytes; resume не добавляет и не переопечатывает receipt. Изменение hook trusted hash по-прежнему должно разрушать verification. [R12]

Это отдельный регрессионный сценарий: materialize → fresh prepare → verify → native run → terminal → resume без изменения config. Запрещено восстанавливать работоспособность resume пересозданием доверенных snapshots: это скроет tampering.

## 10.5. Account/model selection

Configured model ещё не доказывает доступность selected account. Сохранить account-scoped app-server roster и специальные ограничения моделей/ролей из baseline. Не переносить whitelist только из README: точные имена canonical roles и aliases извлечь из адаптера и fixtures. При неизвестной доступности — явная диагностика, не подстановка другой модели. [R01, R03, R13]

# 11. Адаптеры Claude, GLM и Qwen

## 11.1. Общий контракт, отдельные особенности

Trait/enum адаптера должен покрывать describe/validate/materialize/probe/models/limits/prepare/launch, а session — wait/steer/cancel и ownership evidence. Конкретный набор capabilities берётся из baseline каждого runtime. Нельзя заявить capability всем адаптерам ради единообразия интерфейса. Unsupported request отвергается до запуска. [R13]

Допускается общая библиотека stream parsing и Claude-compatible process transport, но model IDs, auth rules, endpoint ownership и failure classifiers остаются runtime-specific. GLM не просто alias имени Claude в config.

## 11.2. Claude

Перенести headless invocation, JSON stream parsing, result envelope, usage, stderr handling, generated settings и явный контроль sources/MCP/plugins. Точные CLI flags зафиксировать fixtures до рефакторинга; недопустимо включить ambient hooks/skills из пользовательского HOME как побочный эффект нового launcher. [R03, R09, R13]

Native credential behavior зависит от наличия account label. Unlabelled использует host credential state; labelled сохраняет изолированную долговечную credential directory. Перенос должен подтвердить, что login и последующий launch используют одну lineage. Не вытаскивать скрытые tokens из credential stores для quota collection: использовать только разрешённый источник OAuth token. API key не трактуется как OAuth token. [R03, R09]

Для resume сохранять native session history; не возвращать устаревший `--no-session-persistence`. Историческая сессия без history должна дать явный отказ continuation, а не новую беседу с пересказом. [R10]

## 11.3. GLM

Повторно использовать проверенные Claude protocol primitives, сохранив pinned Anthropic-compatible endpoint и правильный набор auth environment/Keychain fallbacks из исходного адаптера. Пробу наличия credentials отделить от их получения; никакие значения не записываются в doctor JSON. [R03, R13]

Fixture-набор должен показать, что запуск GLM не использует личный Claude account при отсутствии GLM credential и не меняет global Claude config. Failure classes и limits source проверяются отдельно от Claude даже при общей библиотеке кода.

## 11.4. Qwen

Сохранить headless `stream-json`, sandbox flag и mapping approval режима на granted write/read-only. Model selector не должен ограничиваться локальным enum, если config допускает provider-qualified строку. На macOS перенести корректный выбор реального Git из Xcode/toolchain вместо sandbox-hostile shim. [R03, R13]

Продолжение выбирает точный `--resume <id>`, не «последнюю сессию». При несоответствии session или неизвестной истории нельзя автоматически отправить task ещё раз. Проверить output schema/read roots/effort только в тех комбинациях, которые реально поддерживает baseline adapter. [R10]

## 11.5. Обязательные fixtures каждого движка

На каждый runtime нужны: success, provider error, error-only result, nonzero exit, EOF до completion, malformed JSON, split UTF-8, stderr flood, cancellation, unavailable credentials, missing history, busy session, explicit account и native global где применимо. Live fixtures должны быть очищены от credentials и пользовательских задач; синтетические boundary fixtures маркируются как синтетические, а не как записи реальных запусков.

# 12. Native resume и неизменяемая lineage

## 12.1. Что наследуется

Resume создаёт новый agent_id, сохраняя runtime, model, effort, effective account/home, workdir, grants, read roots, output schema и fast selection. Меняются task, caller binding и совместимый stored timeout; при отсутствии нового timeout наследуется предыдущий. Runtime history остаётся у движка. [R10]

Новая строка имеет parent_agent_id, root_agent_id и sequence. Предыдущая строка не возвращается в running, её transcript/answer/stats не перезаписываются. Terminal predecessor допускает только одного successor, причём только latest run цепочки может продолжаться.

## 12.2. Admission continuation

Сначала replay lookup по исходной caller namespace и request ID, затем проверка predecessor и identity. Matching replay должен вернуть уже admitted child даже при последующем изменении текущего config/directory. Conflicting parent/task/timeout/caller возвращает конфликт, а не создаёт ещё один запуск. [R10, R14]

Проверить process cleanup predecessor, native history availability, сохранённый immutable role/config snapshot и unchanged grants. Статус lost не является разрешением продолжать: unknown ownership или surviving process group блокирует continuation. Failed preparation может передать дальше last known native context только по существующему доказанному правилу, а не автоматически.

## 12.3. Exactly-once граница

Нельзя гарантировать ровно одно выполнение внешнего prompt только SQLite-транзакцией. Есть окно, когда engine принял turn, но подтверждение потеряно. В таком случае сохраняется ambiguous outcome и не выполняется автоматический resend. Идемпотентность admission защищает собственные строки, но не заменяет native operation identity.

Run usage из cumulative counters вычисляется по надёжному baseline; без него значение unknown. Нельзя приписать следующему run все токены предыдущего conversation или обнулить старые stats ради красивой суммы. [R10]

# 13. Socket broker и MCP

## 13.1. Unix-socket сервер

Сохранить AF_UNIX SOCK_STREAM, UTF-8 NDJSON, максимальный frame 1 MiB, запрет JSON-RPC batch arrays и отсутствие ответа на notifications. Request IDs сохраняют допустимые scalar types; bool не становится числом. На одном соединении ответы идут в порядке запросов. [R14]

Lifetime lock защищает socket path. Reclaim допускается при доказанном stale socket: connection refused и неизменившийся inode. Медленный ping, malformed response, timeout или permission error не доказывают, что owner умер. При shutdown удалить только собственный inode и корректно закрыть owner stores.

## 13.2. Давление, fairness и cancellation

Connection count, parser buffers, queued calls и output buffers ограничены. Control lane для start/resume/cancel/steer не стоит за большим transcript read. Reserved connection slot допускает только parsed control method; незавершённый первый frame в этом слоте имеет текущую короткую границу 0.5 секунды. [R14]

Сохранить overload `-32001` и request-deadline `-32002`, помимо обычных JSON-RPC ошибок. У потребителя, который не читает ответ, конечный write deadline. Disconnect клиента отменяет ожидание ответа, но не rollback уже admitted durable run. Зафиксировать точку admission так, чтобы повтор с request_id был безопасным.

## 13.3. Error mapping

| Код | Значение |
|---|---|
| -32700 | Parse error/превышение line limit согласно baseline |
| -32600 | Invalid JSON-RPC request, batch, недопустимый id |
| -32601 | Неизвестный method |
| -32602 | Invalid params |
| -32000 | Domain error с устойчивым `error.data.code` |
| -32001 | Перегрузка |
| -32002 | Истёк deadline запроса |
| -32603 | Internal error с bounded безопасным сообщением |

Точное тело error.data, ограничение длины и protocol disposition после ошибки переносить fixtures. Недопустимо возвращать internal error на любой typed validation failure. [R14]

## 13.4. MCP — proxy, а не второй execution host

Использовать официальный Rust SDK `rmcp` для stdio lifecycle и protocol handling. Callback открывает собственный broker client и обращается к той же domain surface; MCP-процесс не создаёт SQLite store для запуска и не владеет долговечным engine child. При недоступном broker возвращается BrokerUnavailable без local fallback. [R03, R14, W03]

Версию SDK выбирать по проверенной совместимости с реальными клиентами, а не автоматически по largest version. Исходный Python пакет использует `mcp==2.1.1`; это версия SDK, не номер wire protocol. Проверить initialize/discovery, tools/list, tools/call, notifications, cancellation, EOF и все используемые клиентами protocol versions. Поддержка нового MCP стандарта не должна удалять старый путь клиента без решения.

MCP stdout — исключительно протокол. Все diagnostics идут в stderr с redaction. SDK task cancellation не должна протечь в cancellation token уже admitted durable agent.

# 14. Delivery, hooks и Desktop relay

## 14.1. Durable outbox

Перенести binding, notification identity, terminal-event uniqueness, waiting_binding/pending/sending/delivered/retry_wait/failed/cancelled/expired, backoff, lease и ambiguous result. Не отправлять уведомление напрямую из transaction завершения agent: external I/O не держит SQLite lock. [R08, R15]

Claims должны быть условными: worker завершает только свою попытку и свой lease. После crash stale sending корректно возвращается в retry flow по исходным правилам. Повторное подтверждение не создаёт вторую evidence row с тем же attempt number. Retry не бесконечен вопреки текущей expiry semantics; значение max_attempts=0 интерпретируется по baseline, а не по догадке.

## 14.2. Completion notice как безопасный контракт

Сохранить packaged JSON-template и allowlisted guidance. Notice содержит lifecycle identity, status, runtime/model/effort и notification ID. Task, answer и сырой failure prose никогда не становятся его текстом. Missing effort обозначается explicit unspecified, а не default модели. [R15]

Escape controls, Unicode separators, braces и dollar signs без повторной интерполяции. Failure/Advice допустимы только для соответствующих terminal categories и формируются из доверенного справочника. Notice — сообщение о состоянии, не новая пользовательская задача и не одобрение последующих действий. Context handling instructions должны жить в общем contract/doc, а не дублироваться на усмотрение каждого transport.

## 14.3. Immutable evidence

Сохранять classifier, duration, точный exit status либо spawn errno, counts исходных stdout/stderr bytes, truncation flags и bounded redacted tails. Не смешивать exit 127 со spawn ENOENT. Не хранить доставляемый текст, session ID, argv values, environment values, credentials, socket path или host response body в evidence. Tail limit — 4096 UTF-8 bytes; запись — не более 16 KiB. [R02, R08, R15]

Redaction проверяется до persistence, а не при отображении. Редактированный tail должен оставаться корректным UTF-8 после truncation. Для non-queue deliveries last_attempt остаётся null по исходному view contract, а не заполняется фиктивными данными.

## 14.4. Desktop relay — отдельный протокол

Существующий host использует little-endian u32 length prefix, а не NDJSON. Local request ограничен 8192 bytes, host inventory — 8 MiB; host call имеет budget 8 секунд, discovery — общий budget 10 секунд внутри delivery lease. При переносе нельзя применить общий 1 MiB socket parser к этому каналу. [R15]

Поддержать строгие формы legacy v1, selector-rich v2 и failure-aware v3; discovery предпочитает v3, затем v2, затем legacy. Unknown fields/ops отвергаются. Разрешён только фиксированный host tool для completion; arbitrary RPC, task forwarding и произвольный message text в этот канал не добавляются.

## 14.5. Ранний spike pure-Rust host compatibility

Исследовать фактический Desktop handshake и проверку host identity с разрешёнными capabilities. Реализовать минимальную Rust-пробу без бизнес-логики: connect, inventory, единственный допустимый completion call, корректный cleanup и отсутствие рекурсивного wrapper. Проверить на целевом Mac с настоящим Desktop, не на fake socket.

При успехе перенести renderer/validator/framing в Rust и удалить собственный `.cjs` из runtime/package-data. При неуспехе зафиксировать техническую причину и исследовать поддерживаемый native extension/host bridge. Сохранение собственного JS «временно навсегда» не закрывает strict full migration. Внешний host-managed bridge допустим только при явно согласованной границе продукта; отсутствие решения оставляет release gate закрытым. [R15]

## 14.6. Hooks и context

Перенести `hook bind` и `hook context`, нормализацию engine-specific events, поиск agent_id в JSON/MCP envelopes, проверку допустимого transport и transactional binding. Late PostToolUse binding не меняет namespace request_id, использованную при admission. [R05, R14]

Context receipts привязаны к правильной orchestrator session и revision/context key. Не вставлять контекст повторно при каждом connect. Не превращать текст tool_response в shell command, разрешение или свободный маршрут доставки. Claude UDS transport проверяется отдельно от Desktop relay, включая отсутствие session, отказ listener и неоднозначное принятие.

# 15. Capacity, topology, прогнозы и статистика

## 15.1. Сбор и чтение — разные операции

Перенести источники `native`, `codex_appserver`, `codexbar`, `omniroute`, `none`. Collector делает bounded external calls и публикует наблюдения. `limits` и `capacity_order` читают уже committed данные. Нельзя незаметно превратить read API в медленный provider probe или удалять healthy data при failure соседнего account. [R03, R06, R16]

Различать результаты collection: collected, partial, failed, no_data, unsupported. Для supported source пустой успешный ответ не равен успешному сбору. `capacity collect --once` сохраняет JSON report и exit status 2 для оговорённых неуспешных/неполных outcomes. Логи содержат безопасные codes/counts, не provider response body. [R03, R05]

## 15.2. Freshness и идентичность

Freshness определяется source observed_at, а не временем повторного чтения старого cache. Future observation и reset_at, уже наступивший на момент оценки, делают evidence неизвестным. Выбор newest row выполняется по quota identity до применения display cap, иначе активный account может скрыть stale sibling. [R03]

Account labels opaque; absent account не совпадает с literal base/default/shared. Governing windows, model-specific buckets и physical pools сохраняются отдельно. Не складывать алиасы одного пула как независимую доступную мощность.

Reset-cycle grouping допускает текущую небольшую погрешность до одной секунды только когда оба reset ещё были в будущем на latest observation. Нельзя объединять завершившийся cycle со следующим ради сглаживания графика. [R03]

## 15.3. Codex и OmniRoute

Codex app-server пробы выполняются по account независимо. Неправильный присутствующий quota window блокирует его bucket route; healthy sibling windows остаются advisory, но не доказывают отсутствие неизвестного ограничения. Standard и model-specific limits не сливаются в один route. Manual reset credits привязаны только к допустимому upstream bucket. [R03, R14, R16]

OmniRoute freshness берётся из текущего `key_value` cache, namespace `providerLimitsCache`, observation clock `fetchedAt`, не из change-only quota_snapshots. Pool freshness ограничивается старейшим включённым участником. Missing/malformed active participant, future observation, expired reset и превышение 64-row bound не должны молча исчезать из среднего. Docker reader выдаёт лишь разрешённые проценты и timestamps, без credentials/connection identifiers. [R03]

## 15.4. Формула ranking, которую необходимо сохранить

Все вычисления используют один injected `now`. Для known window с burn evidence не менее часа и будущим reset:

```text
projected = remaining_percent - burn_percent_per_hour * hours_to_reset
slack = clamp(projected / 100, -1, 1)
```

Для warmup/thin/no-reset evidence:

```text
slack = clamp(2 * remaining_percent / 100 - 1, -1, 1)
score = 1 + min(slack всех governing windows)
priority = score * effective_weight * reset_credit_multiplier
reset_credit_multiplier = 1 + n / (n + 1)
```

Exhausted window с remaining=0 исключает route **до** scoring и любых multipliers. Unknown/malformed governing window отправляет route в deferred. Weight precedence: account override, затем quota-lane override, затем runtime default; это замены, не произведение весов. У shared-pool aliases используется максимум применимого веса; credits также не суммируются. [R16]

Сохранить deterministic tie-break: priority, score, minimum remaining, limiting reset и alias identity согласно baseline. Limiting key определяется worst slack, не просто ближайшим reset. `insufficient_diversity` означает меньше двух working physical choices; список routes при этом может быть пуст или содержать один элемент.

Пример для теста: fallback remaining 50% даёт slack=0 и score=1; weight=2 и n=1 дают priority=3. При remaining=0 route omitted независимо от weight=100 или credits. Это контрольные входы формулы, не реальные quota measurements.

## 15.5. Числа и отчёты

Finite numbers, percent range, отсутствующее значение и ноль должны различаться. Не превращать unknown в 100%, не округлять до ranking и не сортировать f64 с NaN. Проверить overflow произведения больших положительных весов: решение о reject/defer должно быть явным, с тестом и при необходимости ADR изменения baseline.

RunStats сохраняет input/output/cache/reasoning/total tokens, turns, TTFT, duration, API duration, cost и usage_source, только когда есть достоверные данные. Unknown остаётся null. Измеренные и estimated деньги не смешиваются; отсутствие model price не означает нулевую стоимость. [R03, R08, R10]

# 16. Эксплуатация, диагностика и поставка

## 16.1. Init и doctor

`init` создаёт private home, schema и поставляемые ресурсы идемпотентно, не перезаписывая пользовательский config. Проверить конфликты с symlink/special file, существующие profiles/skills и неполную предыдущую инициализацию. Generated examples не должны содержать локальные credentials или developer paths.

Doctor проверяет config/roles/assets/MCP, доступность binaries и допустимые auth references, native history/snapshot health где применимо, capacity freshness и delivery configuration. Он не запускает новый агент, не чинит trust receipt и не повторяет expensive toolchain probes внутри start. Warnings должны отличать optional integration от реально сломанной required capability. [R02, R09]

## 16.2. Логи

Структурированные события содержат agent_id, attempt, phase, safe reason code и bounded numeric metadata. Не выводить полные StartRequest/LaunchPlan через Debug и не использовать `#[instrument]` без skip для секретных аргументов. Отдельный тип SafeDiagnostic должен исключать arbitrary provider payload. [W18]

Убедиться, что panic/backtrace не выводят credential values, task, stdout или host response. Journaling транскрипта и технический лог — разные каналы: доступ к сохранённым ответам не является разрешением копировать их в diagnostics.

## 16.3. launchd и Linux

Сохранить generators API/capacity/delivery jobs, labels, stdout/stderr paths, RunAtLoad/KeepAlive и передачу только нужных HOME/PATH для headless collector. API job должен сохранить указанную в baseline настройку soft open-file limit 65 536; процесс, запущенный вручную, может иметь другие limits. [R05, R14]

Plist формировать через проверенный serializer либо строго ограниченный XML emitter с escaping, не string interpolation необработанных аргументов. На Linux отсутствие launchd/Keychain должно быть корректной платформенной особенностью, а не падением основных CLI/MCP/socket функций. Systemd unit — отдельное расширение, не обязательная скрытая часть parity.

## 16.4. Sealed releases

Заменить venv-based release на immutable directory с native binary, resources, metadata и COMPLETE marker, сохранив безопасную атомарную смену current и retention живых releases. Старый release нельзя удалять, пока из него работают supervisor/MCP/session processes. Бинарник, docs/assets и schema set относятся к одной версии, не смешиваются через несколько symlinks. [R03, R17]

`xtask` заменяет собственные Python release scripts: проверка version/tag, сборка, упаковка, checksums, smoke installation, release metadata и безопасный prune. Сборка включает фиксированный Cargo.lock. Lock и checksum не являются цифровой подписью или доказательством воспроизводимого binary: attestations/reproducibility проверяются отдельными шагами. [W23, W24]

## 16.5. Целевые платформы

Обязательная baseline-платформа — macOS и Linux. Практическая matrix сборки должна явно перечислить architecture и минимальную OS/libc версию. Рекомендуемые начальные native targets: Apple Silicon macOS и x86_64 Linux; Intel macOS и aarch64 Linux включаются в поддерживаемый release только после собственного CI/live evidence. Наличие target triple в Cargo не доказывает поддержку всех native integrations.

Нельзя объявить macOS поддержку только результатом cross-compile на Linux: Keychain, launchd, signed Desktop host, sandbox paths и process identity требуют проверки на Mac.


## 16.6. Перенос release orchestration целиком

Существующий release script не просто архивирует файлы: он проверяет clean checkout, создаёт/reuses release branch и PR, ждёт обязательные непустые checks на точном head, соблюдает branch protection, проверяет main CI, создаёт annotated tag и позволяет только release workflow публиковать артефакты. Повтор той же операции должен продолжать прерванный release, не передвигать tag и не перезаписывать immutable опубликованную версию. [R17]

Rust xtask должен сохранить этот resumable flow, provenance verification и private deployment journal. Для native binary меняется тип артефакта, но не право обходить CI или публиковать partial release. Политика distribution остаётся GitHub Releases; добавление crates.io, Homebrew, PyPI или container registry не является автоматической частью миграции.

Local deploy проходит candidate smoke, quiescence, остановку admission/periodic jobs, повторную проверку writers, Backup API, COMPLETE/manifest gate, atomic pointer switch и health checks. Уже мигрированная база повторно не мигрируется. Recovery выбирает binary по фактической schema; исходный flow предпочитает безопасный roll-forward, когда database уже продвинулась. Явный restore backup — отдельная операторская операция. [R17]

# 17. Зависимости Rust и политика их выбора

## 17.1. Рекомендуемый набор

Ниже приведены кандидаты, выбранные под конкретные обязанности проекта. Указанная версия означает версию опубликованной документации, проверенную при подготовке плана, а не уже собранный или проверенный совместно dependency graph. Точные версии, feature flags, MSRV и транзитивные зависимости фиксируются на P1 после сборки на целевых платформах. `Cargo.lock` должен быть получен Cargo, а не написан вручную.

| Зависимость | Назначение и предварительный выбор | Ограничение применения |
|---|---|---|
| `tokio` | Async socket/stdio, process I/O, signals, bounded channels; кандидат 1.53.1 [W01] | Не заменяет detached supervisor, процессную идентичность или гарантию reaping |
| `tokio-util` | CancellationToken и ограниченные codecs, если они действительно сокращают handwritten plumbing [W27] | Отмена клиентского запроса отделена от отмены durable run; frame limit задаётся явно |
| `rusqlite` | Существующий SQLite и Backup API; кандидат 0.40.2, features `bundled`, `backup` [W02] | Сохранять owner-thread contract; не вводить ORM и не менять схему автоматически |
| `serde` | Строгие wire/config DTO, derive; кандидат 1.0.229 [W11] | `deny_unknown_fields` использовать осознанно; отдельные правила для legacy payload |
| `serde_json` | NDJSON, native frames, сохранённые JSON-документы; кандидат 1.0.151 [W12] | Стандартная сериализация сама по себе не гарантирует совпадение Python hash bytes |
| `toml` | Config, front matter, native Codex preferences [W13] | Разделять parse/validation/render; не пропускать null/date/nonfinite в native_settings |
| `clap` | CLI parser; кандидат 4.6.7 [W04] | Перехватывать validation errors в существующий JSON envelope; не менять exit codes по умолчанию библиотеки |
| `rmcp` | Официальный Rust MCP SDK, stdio server; кандидат 3.4.0 [W03] | Подтвердить negotiation/cancellation с реальными клиентами; минимально `server` и `transport-io` |
| `rustix` | FD-based filesystem операции, file locks и POSIX primitives; кандидат 1.1.4 [W05] | Не считать wrapper sandbox; платформенные ограничения проверять отдельно |
| `libc` | Узкий FFI для отсутствующих spawn/process-identity API; кандидат 0.2.189 [W25] | Только platform crate; документированные safety invariants и обязательный review |
| `reqwest` | HTTP-источники capacity, если endpoint действительно вызывается напрямую; кандидат 0.13.5 [W06] | Ограничить deadline/body/redirects; не пересылать credentials другому origin; осознанная proxy policy |
| `thiserror` | Типизированные доменные ошибки и стабильное преобразование транспортами [W14] | В публичный текст не вкладывать произвольный provider error/body |
| `sha2` | SHA-256 answer/config/tree manifests [W15] | Сначала определить точные bytes; hash не заменяет containment или разрешение на чтение |
| `getrandom` | Криптографическая случайность для текущего формата IDs и уникальных temporary names [W16] | Сохранить формат IDs; не переходить на UUID/ULID ради удобства |
| `time` | UTC timestamps и формат идентификатора [W17] | Для deadlines использовать monotonic time; Unix epoch в storage сохраняет прежнюю семантику |
| `tracing`, `tracing-subscriber` | Структурированные безопасные operational logs [W18] | Никакого автоматического Debug для request, environment, transcript или auth |
| `plist` | Корректный XML plist для launchd [W26] | Только генерация конфигурации; не добавляет launchd на Linux |

SQLite, TLS и системные библиотеки могут содержать собственный не-Rust код. Это не нарушает миграцию собственного agent-run на Rust: цель — убрать его Python/JavaScript implementation, а не переписать SQLite или ядро ОС. Но native build dependencies и licenses должны быть явно отражены в manifest поставки.

## 17.2. Тестовые и вспомогательные зависимости

| Зависимость | Где использовать | Условие включения |
|---|---|---|
| `proptest` | FSM, arbitrary JSON, пути, quota arithmetic, последовательности store operations [W20] | Сохранять regression seeds и ограничивать входы воспроизводимым профилем |
| `insta` | Golden snapshots wire envelopes, generated settings и safe notices [W21] | Snapshot update не означает одобрения изменения контракта; diff проверяется вручную |
| `tempfile` | Изолированные test homes, DBs и candidate directories [W22] | Не заменяет production no-follow publication protocol |
| `criterion` | Чистые parsers/ranking/serialization benchmarks [W28] | Отделять microbench от end-to-end latency с реальным engine |
| `zeroize` | Узко ограниченные owned secret buffers [W19] | Опционально; не обещать очистку всех копий, системных buffers или чужих библиотек |
| `cargo-deny` | Dependency advisories, sources, licenses и duplication policy [W24] | Версию CI-инструмента закрепить; временные исключения должны иметь причину и срок пересмотра |

`schemars` можно использовать через совместимую интеграцию rmcp для схем, но authoritative schema должна остаться одна. До подтверждения точного соответствия baseline безопаснее держать golden JSON schema assets и проверять генерируемые схемы против них. Это проектное решение о контракте, не отказ от типизации.

## 17.3. Что намеренно не выбирать по умолчанию

Не вводить одновременно SQLx, Diesel и rusqlite; многослойный DB stack усложнит существующие транзакционные гарантии без нужной функции. Не подключать Axum/Tonic для Unix NDJSON только ради популярности. Не добавлять PyO3, embedded Python, JavaScript runtime или `codex exec` wrapper как способ формально получить `.rs` файлы.

`sysinfo` либо другой общий process-monitor crate может быть оценён для диагностики, но не должен автоматически становиться источником birth identity: сначала требуется доказать точность, единицы времени и поведение при denied/unknown на обеих ОС, в том числе чтение старого `psutil.create_time` evidence. Прямой platform seam предпочтительнее, если общий API теряет эти различия. [R18]

Для Keychain сначала перенести точную существующую операцию: выбранные сервис/account selectors, отсутствие фонового interactive prompt, bounded result и safe diagnostics. Новый универсальный credential crate допускается только после проверки parity; менять способ хранения секретов одновременно с языком необязательно.

## 17.4. Процедура фиксации зависимостей

На P1 создать workspace с явным `rust-version` и фиксированным stable toolchain в `rust-toolchain.toml`. Выбрать минимальную версию Rust по пересечению требований реально выбранных crates, а не по номеру последней документации std. Nightly не нужен по умолчанию; отсутствие stable `CommandExt::setsid` закрывается platform/helper design, а не скрытым переключением toolchain. [W07]

Сначала собрать минимальный граф на macOS и Linux; затем добавить runtime features по одному, выполнить unit/transport smoke и сохранить `cargo tree -e features`. Провести license/advisory review, закрепить `Cargo.lock`, собрать с `--locked`, проверить повторную сборку в чистой среде. Проверка `--offline` проводится отдельно после явного vendor/cache provisioning; наличие lock не делает зависимости доступными без сети. [W23, W24]

Для каждого обновления хранить: причину, изменения API/behavior, MSRV delta, feature delta и тесты совместимости. Правило обновления patch/minor не должно обходить MCP negotiation, SQLite и native integration gates. Dependency audit является необходимой проверкой, но не доказательством безопасности всего приложения.

# 18. Поэтапная программа миграции

## 18.1. Порядок этапов и зависимости

План намеренно начинается с воспроизводимого эталона и рискованных стыков. Перенос «сначала весь happy path, потом безопасность» недопустим: так можно получить большую реализацию, которую невозможно безопасно подключить к существующей базе или Desktop.

| Этап | Результат | Предпосылки |
|---|---|---|
| P0 | Замороженный baseline, inventory и исходные regression fixtures | Зафиксированный SHA |
| P1 | Компилируемый workspace, lock, CI и решения по рискованным платформенным стыкам | P0 |
| P2 | Доменные контракты, config, profiles, role plan, canonicalization | P1 |
| P3 | Совместимый SQLite store и migrations | P2 |
| P4 | Безопасные artifacts, snapshots и process identity | P2; store-интеграция после P3 |
| P5 | Durable supervisor и fake-engine вертикальный срез | P3 + P4 |
| P6 | Codex adapter и native resume базового пути | P5 |
| P7 | Claude/GLM/Qwen и их native resume | P5; общий adapter contract из P6 |
| P8 | Полный CLI, socket и MCP поверх общего сервиса | P3–P5; engine parity после P6/P7 |
| P9 | Capacity, delivery, hooks, Desktop native relay | P3 + P8; положительный relay spike P1 |
| P10 | Doctor/init, launchd, native releases, deploy/recovery | P6–P9 |
| P11 | Полная differential, crash, security и platform qualification | P0–P10 |
| P12 | Контролируемое переключение, проверка rollback и финальная поставка | P11 и отдельное разрешение на реальное развёртывание |

P8 получает минимальный тестовый transport раньше финального завершения; P9 capacity и delivery можно вести параллельно после фиксации store/service API. Параллельность не меняет обязательных gates: приёмка P12 требует все строки, а не только критический happy path.

## 18.2. P0 — эталон и карта покрытия

Получить полный checkout точного commit вместе с SQL, JSON/CJS assets, operator guide, scripts и tests. Сохранить machine-readable inventory: путь, тип, hash, imports/resources, назначение и предполагаемое Rust-место. Отдельно перечислить pytest collection IDs и платформенные markers/skips. Снять package metadata и фактические CLI help/JSON responses.

Запустить исходную suite в поддерживаемой Python 3.14 среде. Сохранить команды, exit status и полные bounded test logs. Если baseline падает, оформить исходный дефект/нестабильность, а не «поправить» assertions при переносе. Для timing flakes хранить оба результата и isolated rerun; зелёный rerun не стирает первый failure.

Подготовить sanitized DB fixtures схем 1–16, proofs v1/v2, generated homes, native protocol transcripts и completion notices. Не копировать живые credentials или пользовательские разговоры в публичный corpus. Для каждого образца записать происхождение, версию движка, sanitization и hash.

**Выход:** `baseline.json`, `inventory.json`, `test-map.csv`, `fixtures/`, `baseline-report.md`. **Gate P0:** все файлы/тесты имеют строку учёта, отсутствие live evidence явно помечено, baseline воспроизводим либо каждый failure классифицирован.

## 18.3. P1 — workspace и ранние исследования

Создать рабочие crates, `cargo fmt/check/test`, CI на Mac/Linux и lock-policy. До широкого переноса выполнить четыре коротких по объёму, но обязательных эксперимента: detached spawn/READY/reaping; совместимость process birth с Python; Python-compatible hash serialization; прямой native Rust Desktop host path.

MCP spike должен пройти initialize, tools/list, tools/call, error mapping, cancellation и EOF. Desktop spike работает в выделенной тестовой host-session и не отправляет произвольные сообщения в существующие разговоры. Сохранить versions и доказательство, какой процесс владеет host capability.

**Выход:** минимальная сборка, CI evidence, ADR process launch/canonicalization/MCP/relay. **Gate P1:** невозможность выбранного relay пути или нестабильная process identity блокируют обещание полного parity; эти пункты не закрываются заглушками.

## 18.4. P2 — типы, configuration и права

Перенести enums/newtypes/validation/error codes, 11 tool schemas и view DTO. Реализовать TOML config, aliases встроенных adapters, accounts, native reserved fields, profile front matter и ResolvedRolePlan. Сделать canonical serializer для исторических hash-bound документов.

Решить ADR дополнительных `required_constraints`, правила legacy profile и canonical grants. На этом этапе не нужны реальные credentials и engine execution: всё проверяется pure/differential fixtures. Чтение referenced secret source в config parsing запрещено.

**Выход:** domain/config crates, схемы, golden corpus, config migration note. **Gate P2:** valid/invalid fixtures дают ожидаемые решения, неизвестные ключи и неподдерживаемые ограничения не приводят к расширению полномочий.

## 18.5. P3 — durable state

Перенести schema assets и migrations без изменения schema v16. Реализовать owner connections, transactions admission/replay/lineage/terminal+delivery, projections, pagination, command claims, run_stats и capacity snapshots. Добавить тесты двух процессов и crash между writes.

**Выход:** store crate и read/write compatibility report. **Gate P3:** Rust читает исторические fixtures; старый поддерживаемый binary читает Rust-written совместимые записи в проверенной matrix; newer schema и foreign DB отклоняются; migration interruption не создаёт half-applied schema.

## 18.6. P4 — файлы и process identity

Реализовать anchored no-follow operations, durable publication, proof reader/sealer, tree/index/config snapshots и strict recovery inspection. Перенести birth observation как отдельную платформенную функцию с enum verdicts. Сначала проверить синтетические races, затем fixture shared with Python.

**Выход:** platform/artifact modules, fault injection seams, filesystem/process test report. **Gate P4:** corrupt proof, symlink swap, unknown PID и missing birth не превращаются в success или право послать signal.

## 18.7. P5 — supervisor без реального провайдера

Собрать вертикальный срез: admission → отдельный supervisor → READY → fake engine → transcript/proof → terminal transaction. Fake engine является тестовым Rust-бинарником с управляемыми задержками, partial frames, exit statuses, descendants и faults. Никаких production bypass flags для него в публичном CLI.

Добавить cancel/steer outbox, lost reconciliation, daemon/CLI disconnect и bounded bootstrap. Проверить high-load чтение одновременно с cancel. Все исходы должны иметь evidence, даже когда запуск завершился до получения CLI-ответа.

**Выход:** первый end-to-end executable slice и crash matrix. **Gate P5:** supervisor переживает смерть CLI/broker, не завершает runtime из-за возраста, не сигналит unowned PID и не сообщает success только по exit=0.

## 18.8. P6/P7 — runtime adapters

Codex переносить как app-server session state machine, не как строковую команду. После рабочего start добавить permissions, rosters/accounts, streaming, interruption, resume и исправление trust receipt. Claude/GLM/Qwen переносятся тем же контрактом, но сохраняют собственные parser/auth/sandbox/native-history особенности.

Каждому adapter нужен набор recorded fixtures и изолированный live smoke с разрешёнными credentials. Fake engine подтверждает supervisor, но не подтверждает реальные flags, host sandbox и протокол конкретного движка.

**Выход:** четыре adapters и их capability matrices. **Gate P6/P7:** ни одна baseline capability не осталась silently ignored; отсутствующая функция возвращает явную ошибку и сохраняет этап незавершённым.

## 18.9. P8 — три интерфейса

Перенести полный parser CLI и service bindings; socket framing, locks, overload/deadline semantics, control/read lanes; MCP на rmcp как тонкий proxy. Добавить table-driven parity tests, клиентские disconnect races, long polls и exact error codes.

**Выход:** CLI/API/MCP integration suite и wire compatibility report. **Gate P8:** все 11 tools имеют одинаковый результат через общий service; служебные API и CLI-команды отдельно покрыты; `start` не имеет local fallback при недоступном broker.

## 18.10. P9 — квоты и уведомления

Capacity делится на parsing наблюдений, storage/topology, pure forecast/rank и side-effecting collectors. Delivery делится на claims/evidence, safe template, transport send и ambiguity handling. Hooks/context подключаются только через опубликованные service/store interfaces.

Native Desktop relay должен пройти host smoke и interop v1/v2/v3. Протокол может быть корректен на fake host, но этап остаётся незавершённым без проверки настоящего Desktop boundary.

**Выход:** все источники quota, ranking parity, delivery outbox и native host integration. **Gate P9:** exhaustion/unknown не превращаются в usable route; двойной dispatch не дублирует owned claim; секретные поля не попадают в notice/evidence.

## 18.11. P10 — operational parity

Перенести init/doctor/docs, launchd generators и release/deploy tooling. Проверить установку на машину без Python как зависимости agent-run. Изолированная установка должна находить встроенные assets независимо от cwd и source checkout.

**Выход:** candidate distribution, native xtask, deployment journal и recovery runbook. **Gate P10:** published package не содержит скрытой делегации старому Python/Node-коду; install/switch/rollback drill проходит на тестовом home.

## 18.12. P11/P12 — приёмка и переключение

Выполнить полный набор раздела 20, включая live/native и macOS-specific scenarios. Получить отчёт о покрытии исходных test behaviors, закрыть blockers и сформировать release candidate из точного SHA. Затем провести runbook раздела 21 на копии окружения и только после отдельной авторизации — на production home.

**Выход:** подписанный ответственным review checklist, evidence bundle, release archive и post-cutover report. **Gate:** нет открытых parity/security blockers; всё заявленное supported реально проверено. Документ, crate scaffold или только `cargo test` на mocks не закрывает P12.

# 19. Организация исполнения и контроль прогресса

## 19.1. Единица работы

Задание должно быть достаточно маленьким, чтобы завершиться конкретным diff и тестом, но не отрывать invariant от реализации. Например, «proof v2 reader + corrupt/missing sidecar tests» — подходящая единица; «перенести весь service.py» или «написать безопасность» — нет.

Карточка содержит ID, baseline paths, контракт, target modules, prerequisites, fixture IDs, реализацию и способ проверки. Статусы: `planned`, `in_progress`, `implemented`, `verified`, `blocked`. `implemented` не равно `verified`. При отсутствии live-проверки используется явный отдельный флаг, а не скрытый зелёный статус.

## 19.2. Что сохранять после каждого законченного фрагмента

Сохранить исходники, tests, краткое решение и фактически выполненные команды с кодами возврата. Обновить `migration/status.md` и coverage matrix. Не хранить единственную карту проекта в контексте агента или переписке: другой исполнитель должен продолжить с файлами, не перечитывая весь чат.

| Артефакт контроля | Назначение |
|---|---|
| `migration/baseline.json` | SHA Python-базы, versions, даты получения fixtures |
| `migration/architecture.md` | Карта crate/module boundaries и доверенных стыков |
| `migration/tasks.csv` | ID, dependency, status, commit, reviewer, evidence |
| `migration/compatibility.csv` | Исходный контракт → Rust implementation → tests → результат |
| `migration/adr/` | Решения и допускаемые намеренные отличия |
| `migration/evidence/` | Sanitized test reports, commands, environment metadata |
| `migration/status.md` | Что реально завершено, следующий шаг, блокеры |

## 19.3. Параллельная работа

После P2 разделить работу на store, platform/artifacts, adapters, transports и capacity/delivery. Общие DTO и schema assets изменяет один назначенный владелец; изменения проходят короткий contract review до merge. Adapters не получают прямой доступ к store connection и не самостоятельно определяют terminal status.

Каждая ветка использует собственный temporary home и DB. Нельзя параллельно тестировать два execution host на одном production `~/.agent-run`, одну native session или общий release current. Merge выполняется только после интеграционных тестов связанного среза, а не суммирования зелёных unit suites отдельных веток.

## 19.4. Definition of done отдельного задания

Работа завершена, когда diff воспроизводится из указанного commit, контракты покрыты положительным и отрицательным тестом, test command успешно завершился, нет новых secrets/debug leaks и обновлена документация. Для platform/native изменений добавляется evidence соответствующей ОС/движка. «Код выглядит правильным» и «агент сказал, что проверил» не являются результатом.

Оценку календарных сроков делать только после P0/P1 на основании реального inventory и результатов spikes. В этом плане нет выдуманных сроков, процентов готовности или обещания ускорения разработки на Rust.

# 20. Программа испытаний и доказательства совместимости

## 20.1. Уровни проверки

Тестовая программа состоит из четырёх независимых слоёв. Pure/unit tests проверяют правила без ОС и провайдера. Process/filesystem tests проверяют реальные FD, SQLite, spawn и signals в изолированной среде. Differential tests сравнивают Python baseline и Rust на одинаковых входах. Live integration подтверждает конкретный engine/OS/host boundary. Ни один слой не заменяет остальные.

Все сценарии ниже являются **планом испытаний**, а не уже существующими или выполненными Rust-тестами. Идентификаторы `Txx` предназначены для трассируемости. Их нужно связать с существующими Python test IDs на P0 и с будущими Rust test names после реализации.

## 20.2. Differential harness

Каждый deterministic fixture запускается отдельно через Python reference и Rust candidate. Сравнение фиксирует типы, наличие полей, null/omission, status/error codes, курсоры, exact bytes там, где они подписаны hash, и порядок там, где он часть контракта. Сравнивать только отображаемую строку JSON недостаточно.

Нормализация разрешена лишь для явно недетерминированных полей: сгенерированный ID, PID, temporary root, injected timestamp. Их необходимо согласованно переименовывать, сохраняя связи между объектами. Нельзя маскировать все timestamps, hashes, неизвестные поля или ошибки, иначе harness скроет существенное расхождение.

Два кандидата никогда не отправляют один и тот же live prompt в один native thread ради сравнения. Для внешних эффектов применяются записанные протоколы или отдельные тестовые sessions/workdirs. Для DB используются клоны fixture и фиксированный clock, а не два конкурирующих execution host над одной пользовательской базой.

## 20.3. Контракты, configuration и типы

| ID | Сценарий и обязательное утверждение |
|---|---|
| T01 | Полные tools schemas одинаковы для service/socket/MCP; tools ровно 11, без пропусков и транспортных копий |
| T02 | Неизвестный tool, лишний аргумент и отсутствующий required field дают правильный typed error |
| T03 | Boolean не принимается как integer/timeout/weight; NaN, infinity и отрицательные недопустимые значения отклоняются |
| T04 | Agent ID сохраняет длину, дату, lowercase hex и формат; traversal-подобные значения отклоняются |
| T05 | Config unknown key на любом проверяемом уровне отклоняется без вывода значения секрета |
| T06 | Legacy profile ограничивает request.write; canonical role использует собственный grant; разные режимы не смешиваются |
| T07 | Неполный canonical front matter, неизвестные constraints и missing skill/MCP отклоняются до admission |
| T08 | Дополнительные required_constraints соответствуют принятому ADR и не теряются при resume |
| T09 | Native reserved roots нельзя переопределить через вложенные таблицы, literal dotted keys или programmatic config |
| T10 | Обычный native tuning сохраняет типы и меняет revision; dates/null/nonfinite не проходят |
| T11 | Account omitted использует global, а labels base/default/shared остаются отличными scoped identities |
| T12 | Host PATH/toolchain env сохраняется по контракту; unselected credential env отсутствует; config parsing не читает secret source |

## 20.4. State и миграции

| ID | Сценарий и обязательное утверждение |
|---|---|
| T13 | Fresh DB создаётся в v16; schema objects, constraints и indexes соответствуют baseline |
| T14 | Каждый валидный fixture v1–v15 мигрирует в v16 с сохранением строк и внешних ключей |
| T15 | Newer schema, чужая БД и отсутствующие обязательные v1 tables отклоняются без reinitialize |
| T16 | Crash перед/во время/после migration commit оставляет согласованную версию и понятный backup/recovery state |
| T17 | Backup API сохраняет committed WAL pages; восстановленная копия проходит integrity и foreign-key checks |
| T18 | Одновременные start с одинаковым request_id возвращают одну admission; conflicting request даёт conflict |
| T19 | Late binding не меняет первоначальную request namespace; unbound replay работает через новое соединение |
| T20 | Global/runtime active limits проверяются атомарно при конкурирующих admissions |
| T21 | Terminal transition и delivery row появляются вместе; повтор обработки не создаёт второе уведомление |
| T22 | Два resume одного parent не создают ветвление; latest-terminal и unique child ограничения сохраняются |
| T23 | Команды claim/complete сохраняют порядок и ownership; duplicate cancel не ломает FSM |
| T24 | Pagination exact total, cursor/revision и nullable projections соответствуют baseline на пустой и большой истории |

## 20.5. Процессы и lifecycle

| ID | Сценарий и обязательное утверждение |
|---|---|
| T25 | Supervisor записывает PID/birth и READY до slow auth/materialization; start возвращает после ownership handoff |
| T26 | Клиент закрывается до/после READY; уже принятый durable run не зависит от его lifetime |
| T27 | Broker завершается после admission; подтверждённый supervisor продолжает выполнение и записывает outcome |
| T28 | Bootstrap child не читает pipe; parent не зависает бесконечно, bounded error/cancellation соблюдаются |
| T29 | Engine exit до group discovery не считается автоматическим success/failure без остальных evidence |
| T30 | Unknown/denied process observation не маркирует run lost и не разрешает signal |
| T31 | PID reuse даёт reused; stale identity не сигналится; missing PID без birth допускает dead verdict |
| T32 | Cancel сигналит только доказанную исходную process group; surviving group исключает успешное завершение |
| T33 | Escaped descendant наблюдается отдельно и не сигналится индивидуально; cleanup.confirmed не подменяет runtime outcome |
| T34 | Cancellation во время prepare/spawn/stream/terminal race оставляет допустимый FSM и owned cleanup evidence |
| T35 | Длительное молчание и legacy timeout не завершают runtime; watcher timeout завершает только ожидание |
| T36 | Завершённые child processes reap-ятся; число zombies/FD не растёт после повторяемой серии запусков |

## 20.6. Artifacts и snapshots

| ID | Сценарий и обязательное утверждение |
|---|---|
| T37 | Proof v2 связывает exact payload name/size/hash/media type/version; исходные UTF-8 bytes не меняются |
| T38 | V2 marker без sidecar и malformed proof не downgrade-ятся в legacy даже при sentinel в тексте |
| T39 | Legacy completion требует exact terminal frame; sentinel внутри ответа не доказывает завершение |
| T40 | Payload выше 16 MiB отклоняется; ответ выше inline limit проверяется целиком, но не возвращается целиком |
| T41 | Invalid UTF-8, file shrink/growth и hash mismatch различаются как ошибки чтения/целостности |
| T42 | Symlink на файл и любой intermediate directory, special file и path escape отклоняются |
| T43 | Swap имени между проверкой и open не выводит чтение за anchored root |
| T44 | Crash на marker/payload/proof/fsync границах никогда не оставляет неподтверждённый answer как succeeded |
| T45 | Snapshot tree с orphan/missing/type/hash mismatch не чинится автоматически и не считается verified |
| T46 | Python hash-bound JSON с Unicode, escapes, floats, null и упорядоченными ключами читается без смены identity |
| T47 | Plugin asset traversal/globs/ambiguous basename/undeclared content не обходят declared snapshot contract |
| T48 | Codex trust receipt записывается до freeze на fresh start; resume и tampered hooks не переопечатываются |

## 20.7. Движки и продолжения

| ID | Сценарий и обязательное утверждение |
|---|---|
| T49 | Codex initialize/thread/start/turn/start сохраняют request correlation и правильный protocol lifecycle |
| T50 | Fragmented deltas, повторяющиеся куски и whitespace journaling совпадают с reference; idle poll не завершает stream |
| T51 | Canonical completed item не дублирует уже сохранённый текст; старый turn не завершает новый run |
| T52 | Неожиданный app-server EOF — transport failure; explicit error kind/code имеют установленный приоритет |
| T53 | Projects profile и managed requirements совпадают с granted roots/network; неверное echo приводит к отказу |
| T54 | Runtime/model/account roster gate, fast/effort/output_schema и ограничение специальных моделей сохраняются |
| T55 | Claude unlabelled и labelled credential environments различаются по контракту; ambient MCP/settings не расширяют роль |
| T56 | GLM endpoint/auth wiring не смешивается с обычным Claude; ответы и failures правильно классифицируются |
| T57 | Qwen sandbox/approval/read-root flags и macOS Git path проходят native smoke, не только argv snapshot |
| T58 | Native resume использует точный session ID, унаследованные grants/home/effort; отсутствующая history даёт отказ |
| T59 | Неоднозначно отправленный prompt не переотправляется автоматически; новый разговор не заменяет отсутствующую history |
| T60 | Cumulative usage учитывается только с доказанным baseline, иначе null; прошлые run_stats не переписываются |

## 20.8. Транспорты, уведомления и hooks

| ID | Сценарий и обязательное утверждение |
|---|---|
| T61 | Partial/oversized/invalid NDJSON, bad id и batch array дают ожидаемые коды и не раздувают память |
| T62 | Notification без id не получает response; на одном соединении сохраняется порядок ответов |
| T63 | Read overload не лишает control lane возможности принять cancel; reserved slot освобождается при неполном первом frame |
| T64 | Long poll/wait не удерживает DB transaction или owner lane до истечения всего ожидания |
| T65 | Stale socket reclaim требует ECONNREFUSED и того же inode; slow/malformed ping не доказывает смерть owner |
| T66 | SIGTERM останавливает admission, завершает queued requests и закрывает stores в owner context |
| T67 | MCP initialize/tools/call/error/EOF/cancel работают с заявленными версиями клиентов; stdout содержит только protocol |
| T68 | Outbox lease нельзя завершить чужим owner; crash/retry/expiry/ambiguous acceptance сохраняют правильный state |
| T69 | Каждая evidence row immutable и атомарна с delivery verdict; session/task/answer/env отсутствуют |
| T70 | Notice использует только allowlisted template/guidance; Unicode/control escaping не позволяет внедрить дополнительную инструкцию |
| T71 | Relay u32LE frame, v1/v2/v3 shapes, лимиты и deadlines проверены на fake host и отдельном настоящем Desktop smoke |
| T72 | Hook bind/context, поздняя привязка и receipt дедупликация не создают новую request identity и не переполняют bounded context |

## 20.9. Capacity и эксплуатация

| ID | Сценарий и обязательное утверждение |
|---|---|
| T73 | Native/codex_appserver/codexbar/omniroute/none имеют отдельные collected/partial/failed/no_data/unsupported исходы |
| T74 | Failure одного account не удаляет свежие sibling scopes; TTL старой topology не продлевается фиктивно |
| T75 | Future observation, expired reset и missing governing window делают route неизвестным, а не usable |
| T76 | Reset jitter в пределах допустимого правила не сливает новый цикл со старым; raw timestamps не меняются |
| T77 | OmniRoute читает current cache freshness, отвергает overflow и malformed included members; secrets не пересекают reader boundary |
| T78 | Ranking formula, worst window, absolute weight overrides, pool aliases, credits и tie-break совпадают с fixtures |
| T79 | Exhausted route нельзя оживить multiplier/credits; unknown не подменяется нулевой стоимостью или полной квотой |
| T80 | `collect --once` печатает report и возвращает корректный nonzero при partial/failed/empty supported source |
| T81 | Init не перезаписывает config, doctor не запускает задачу и не требует optional интеграции как mandatory |
| T82 | launchd XML escaping, labels, HOME/PATH и API FD limit сохраняются; unsupported platform обрабатывается явно |
| T83 | Release candidate работает вне source tree без собственного Python/CJS; все resources найдены |
| T84 | Cutover/retry/roll-forward/explicit restore проходят crash drill, не запускают old binary на newer schema и не теряют current pointer evidence |

## 20.10. Нагрузочные и длительные проверки

Отдельно измерить cold CLI start, broker idle RSS/CPU, admission-to-READY, latency cancel при read overload, ответ на limits/ranking, DB projection на больших histories, число открытых FD и объём transient buffers. Сравнение производительности проводить с одинаковыми fixtures, machine/OS/toolchain и release mode. Engine generation latency отделять от agent-run overhead.

Численные budgets назначаются после baseline measurement. Предлагаемое правило приёмки: не допускать регрессии critical control latency относительно baseline, неограниченного роста памяти/FD и скрытого уменьшения входных лимитов ради быстрого теста. Это целевые критерии, а не уже измеренное преимущество Rust. Для каждого p50/p95/p99 отчёта фиксируются размер выборки, нагрузка и полная процедура измерения.

Длительный тест многократно выполняет start/cancel/resume/delivery/restart на fake engine, меняет порядок interleavings и проверяет invariants после каждого commit. Seed и минимизированная последовательность сохраняются. Тест не должен зависеть от ночного live-провайдера или неограниченно расходовать квоты.

## 20.11. Формат отчёта о проверке

Каждый report содержит commit candidate, baseline SHA, ОС/architecture, Rust/toolchain/crate lock hash, версию движка или fixture, команду, exit status, пройденные scenario IDs и список skips с причинами. Unsupported и not tested различаются. Один happy-path запуск не даёт права объявить полную поддержку модели/аккаунта/платформы.

Обязательные security/process tests не разрешается «временно» отключать для release. Допускаемые platform skips перечисляются заранее в matrix: например, Keychain на Linux. Mac-only тест, пропущенный из-за отсутствия Mac runner, остаётся непроверенным требованием.

# 21. Переключение на Rust и восстановление

## 21.1. Стратегия без одновременной смены всех контрактов

Предпочтительный первый native release сохраняет schema v16, wire envelopes и ресурсные formats. В development разрешён Python oracle, но installed runtime запускает только Rust-код agent-run. Перед включением реального исполнения candidate сначала читает копию state и проходит isolated dry-run/smoke. Read-only shadow не означает разрешение одновременно запускать оба broker над одним home.

Переключение production является отдельным действием владельца системы. Этот документ не запускает deployment и не даёт coding-агенту разрешения останавливать работающие пользовательские задачи.

## 21.2. Preflight

Убедиться, что candidate соответствует принятому SHA, COMPLETE/manifest/checksums/provenance проверены, все P11 gates закрыты, runtime binaries и host integrations поддержаны. Сохранить версии текущего и нового binary, список реально загруженных launchd jobs, пути конфигурации, schema version и расположение rollback package.

Проверить доступное место для backup/artifacts, права на home, отсутствие concurrent release operation и читаемость текущего manifest. Сделать инвентаризацию native histories и external auth references. Backup agent-run state не обязан содержать внешние credentials: их не следует копировать в общий архив. При этом план восстановления обязан указать, какие внешние histories и credential stores необходимы для resume.

## 21.3. Quiescence и резервная копия

Закрыть новую admission штатным maintenance/deployment механизмом и дождаться завершения существующих runs либо получить отдельное решение об их явной отмене. Прекратить periodic capacity/delivery writes и проверить связанные legacy workflow writers по process birth, даже если workflow больше не используется текущим продуктом. Список сохранённых jobs нужен для точного восстановления их прежнего состояния. [R17]

Зарезервировать writer boundary, повторно проверить отсутствие новых активных rows/processes и остановить broker в правильном порядке. Unknown/denied writer нельзя автоматически признать мёртвым. Сделать SQLite Backup API snapshot в отдельный private retained backup, затем проверить его integrity и schema. Не полагаться на копирование только `state.db` при существующем WAL. [R08, W08, W09]

Сохранить config, metadata/pointers и согласованный набор owned answer/snapshot/history artifacts, которые нужны для восстановления. Выполнить перечень файлов с hash; не создавать публичный ZIP пользовательского home. Нативные runtime histories могут жить вне DB: восстановление одной базы без них не гарантирует native resume.

## 21.4. Порядок переключения

| Шаг | Действие | Проверяемое свидетельство |
|---|---|---|
| C1 | Записать private deployment journal с old/new release, backup и phase | Журнал читабелен после аварийного завершения |
| C2 | Проверить состояние DB и выполнить только действительно нужные migrations | Версия и integrity соответствуют target; no-op для v16→v16 |
| C3 | Убедиться, что candidate release полностью запечатан | Binary/resources/manifest согласованы |
| C4 | Атомарно сменить current pointer на target | Pointer указывает на целый immutable release |
| C5 | Восстановить ранее загруженные services/jobs | Не включены jobs, которые до update были выключены |
| C6 | Дождаться API readiness, проверить schema, tools, doctor | Liveness и capability discovery соответствуют target |
| C7 | Выполнить безопасный isolated smoke и проверить answer proof | Не требуется вмешиваться в пользовательский conversation |
| C8 | Переподключить MCP hosts штатным способом | Новый proxy использует target binary; старые hosts не убиты произвольно |
| C9 | Отметить commit deployment и сохранить backup/old release | Есть воспроизводимый recovery path |

Новый release не должен брать supervisor executable через плавающий `current` для уже принятого запуска: run привязывается к конкретному release. Retention не удаляет старые files, пока ими пользуются живые owners.

## 21.5. Rollback и roll-forward — разные процедуры

Если database/schema не менялись и compatibility matrix подтверждает чтение новых rows старым release, после quiescence допустим binary rollback с сохранением текущих данных. Но сохранённая schema v16 не доказывает семантическую совместимость всех payload versions; проверяется и формат новых snapshot/identity документов.

Если schema или обязательный payload format продвинулись, нельзя просто вернуть old pointer. Существующий release flow выбирает compatible binary по фактическому state и предпочитает roll-forward. Нужно продолжить journal с target version либо использовать исправленный compatible build. [R17]

Explicit restore backup требует остановки всех writers и осознанного решения о данных, появившихся после backup. Восстанавливаются совместимые DB, owned artifacts/config и release вместе. Нельзя «для безопасности» затереть новые completed runs без предупреждения. WAL/SHM очищаются или восстанавливаются только в согласованной offline процедуре; live sidecars не удаляются.

## 21.6. Recovery drills

На disposable home прервать deploy после каждой фазы C1–C9: до DB change, после commit, до/после pointer switch, между загрузкой jobs и health check. Повтор команды должен распознать phase, не повторять уже завершённую migration, не переписывать immutable release/tag и не объявлять успех при остановленных services.

Отдельно проверить corrupt candidate, missing assets, DB busy, insufficient disk, permission denied, failed readiness и потерю внешнего auth/history path. Итогом является journal с понятной следующей операцией, а не общий ответ «попробуйте ещё раз».

# 22. Реестр рисков

Приоритет ниже — проектная оценка последствий ошибки для agent-run, а не статистически измеренная вероятность. «Блокирующий» означает, что без закрытия риска нельзя принимать полный native release.

| ID / приоритет | Риск | Предотвращение и условие закрытия |
|---|---|---|
| K01 · блокирующий | Rust не получает разрешённый Desktop host capability вместо signed Node | Ранний P1 spike и T71 на реальном host; без evidence полный parity не заявляется |
| K02 · блокирующий | PID reuse либо неточная birth time приводит к signal чужого процесса | Platform seam, fixture Python↔Rust, T30–T33 на Mac/Linux; unknown остаётся unknown |
| K03 · блокирующий | Fork/pre_exec в многопоточном broker deadlock-ится или выполняет небезопасную работу | Posix-spawn-first либо audited helper; P1 experiment и T25–T28 |
| K04 · блокирующий | Изменение транзакций создаёт двойной launch/resume/delivery | Store-owned atomic operations и T18–T23 с конкурентными процессами |
| K05 · блокирующий | Несовпадение canonical JSON ломает resume и старые snapshots | Отдельный compatibility serializer, exact golden bytes, T46/T58 |
| K06 · блокирующий | Path/symlink race позволяет читать чужой secret или подменить proof | Anchored descriptors, bounded regular-file reads, T42–T44 |
| K07 · блокирующий | Exit=0, EOF либо текст с sentinel ошибочно становятся success | Независимое outcome verification, T29/T37–T44/T52 |
| K08 · высокий | Перенос role/write/native settings незаметно расширяет полномочия | Config/policy ADR, strict validation, T05–T12/T53 |
| K09 · высокий | Disconnect MCP или long poll отменяет durable run/блокирует cancel | Lifetime separation, две bounded lanes, T26/T63–T67 |
| K10 · высокий | Fault одного account скрывает governing quota либо превращает stale data в fresh | Scoped topology, source observation clock, T74–T79 |
| K11 · высокий | Provider/task/credential prose попадает в completion notice или diagnostics | Тип SafeDiagnostic, allowlist template, negative leak fixtures T69/T70 |
| K12 · высокий | Port live stream меняет whitespace, дублирует final item или теряет хвост | Recorded fragmented protocol fixtures и native smoke T50/T51 |
| K13 · высокий | Старые docs/watchdog comments используются вместо текущего кода | Замороженный baseline, conflict log §2.2, no automatic runtime deadlines T35 |
| K14 · высокий | Release switch удаляет файлы живого supervisor или запускает несовместимый binary | Immutable release pinning, quiescence, journal и T83/T84 |
| K15 · высокий | Оставшийся Python/CJS helper скрыт в hook/config/installer | Static asset inventory и clean-machine runtime dependency audit P10 |
| K16 · высокий | Существующий внешний Python adapter не представлен встроенным alias | Реальный extension inventory; порт либо явно принятый отдельный внешний ABI |
| K17 · средний | Избыточный async/DB pool усложняет ordering и debugging | Фиксированные owner lanes, bounded queues, короткие транзакции, no await внутри transaction |
| K18 · средний | Документация crate новее совместимого графа или MSRV | P1 build matrix, lock, features review; patch-кандидат не считать доказательством совместимости |

Для каждого открытого риска назначается владелец, evidence path и следующая проверка. Обход gate через README disclaimer не закрывает риск: заявляемый release scope должен соответствовать фактическому результату.

# 23. Реестр архитектурных решений

## 23.1. Решения, которые предлагается принять сразу

| ADR | Предлагаемое решение | Причина и доказательство |
|---|---|---|
| A01 | Один native application binary с внутренними Rust modes | Общая версия/assets; supervisor не зависит от Python entry point |
| A02 | Восемь workspace members с ограниченными dependency directions | Изоляция домена, platform unsafe и transport plumbing; §4 |
| A03 | rusqlite owner-thread store; schema v16 сохраняется | Минимум изменений вокруг существующей модели долговечного состояния; T13–T24 |
| A04 | Один tool registry; MCP остаётся broker proxy | Не допускает drift и второго launch host; T01/T67 |
| A05 | Rust-only собственные helpers/release tools; Python oracle только development | Соответствие определению полной миграции; T83 |
| A06 | No automatic runtime deadline; timeout meanings раздельны | Текущий lifetime contract; T35 |
| A07 | No-follow artifact API и отдельный historical serializer | Совместимость proof/resume без ослабления path boundary; T37–T48 |
| A08 | Quiescent cutover, retained backup и journal, без live handoff owners | Уменьшает риск совместного владения; T84 |

Эти строки — рекомендации плана. После реализации ADR должен содержать commit, alternatives, consequences и фактический результат, а не просто отметку «approved» без проверки.

## 23.2. Решения, требующие evidence до фиксации

| ADR | Вопрос | Рекомендуемое исходное правило / gate |
|---|---|---|
| A09 | Native Desktop replacement | Ничего не утверждать о host admission без P1 live experiment; собственный `.cjs` не оставлять под видом pure Rust |
| A10 | Spawn backend и process birth precision | Stable Rust + platform module; выбрать после Mac/Linux tests и Python identity interop |
| A11 | Request.required_constraints у canonical role | Рекомендуется union role ∪ request как явное ужесточение; принять только отдельным изменением контракта с regression test, не выдавать за byte-for-byte baseline |
| A12 | Historical JSON canonicalization | Сохранить exact Python serialization для исторических formats; новое versioned encoding разрешать только с dual-read и migration plan |
| A13 | Пользовательские external Python adapters | Инвентаризировать; baseline built-ins aliases сохранить, arbitrary import не имитировать |
| A14 | MCP SDK patch/features и supported client protocols | Решение по interop matrix, не по совпадению номеров Python/Rust SDK |
| A15 | Список OS/architecture/minimum versions | Обязательны Mac/Linux; каждую architecture/minimum version подтверждать runner/live evidence |
| A16 | Большие weights и числовой overflow | Явный reject/defer с typed error; не позволять Infinity участвовать в сортировке |

Если безопасное намеренное отличие принято, его нужно перечислить в release compatibility notes и проверить старое/новое поведение. Нельзя одновременно утверждать абсолютную побайтовую совместимость и менять такой контракт без указания исключения.

## 23.3. Вопросы, которые не должны тормозить начало

Стиль имён внутренних модулей, общий logger formatter и точное деление небольших utility functions не блокируют P0/P1. В отличие от них host capability, process ownership, schema/payload compatibility и semantic grants требуют решения до интеграционного release. Не расходовать работу на косметическую архитектуру, пока не доказаны эти стыки.

# 24. Итоговая приёмка и состав результата

## 24.1. Definition of complete migration

Полная миграция принимается по совокупности свойств, а не по числу написанных Rust-строк. Весь собственный исполняемый код в Rust; четыре движка и все baseline capabilities поддержаны; interfaces и persisted state совместимы в документированной matrix; данные и полномочия не теряются; release/recovery работают; требуемые native integrations проверены на поддерживаемых ОС.

| Gate | Обязательное свидетельство |
|---|---|
| G1 · Область работ | Inventory не содержит неперенесённых runtime/helpers без явно принятого решения |
| G2 · Сборка | Чистая locked сборка и тесты на каждой поддерживаемой платформе |
| G3 · Контракты | Tool/CLI/wire golden tests и документированный список intentional differences |
| G4 · Данные | DB/proof/snapshot/history fixtures, migration and restore reports |
| G5 · Владение | READY/reuse/cancel/crash/reaping scenarios с реальной ОС |
| G6 · Движки | Recorded fixture parity и разрешённый live smoke каждого runtime |
| G7 · Интеграции | Настоящие MCP/Claude UDS/Desktop/collector smokes в заявленной matrix |
| G8 · Безопасность | No secret leaks, no path escape, no false success, no unowned signals |
| G9 · Эксплуатация | Native package install, doctor, launchd, deploy/retry/recovery drill |
| G10 · Передача | Source archive, lock, docs, tests, evidence index, известные ограничения |

Не принимать «готово», если full Rust не включает relay/release hooks, старые agents видны только в списке, но не читаются proofs, resume создаёт новый разговор, либо тесты покрывают только fake engine. Не принимать положительный `cargo test` с отключёнными критическими scenarios.

## 24.2. Будущий ZIP исходников

Архив итоговой реализации должен содержать Cargo workspace, lock/toolchain metadata, Rust sources, SQL migrations и schema, packaged templates/docs/assets, tests и sanitized fixtures, native release tooling, CI, LICENSE, README с установкой и этот план с фактическим status update. Top-level manifest указывает baseline SHA и native implementation SHA.

Не включать `target/`, `.venv`, `node_modules`, `.git`, credentials, local config с секретами, пользовательские tasks/transcripts, runtime histories и production `state.db`. Checksums и evidence index позволяют проверить, что отчёт относится именно к выданным исходникам. Компилируемые platform binaries могут поставляться отдельными release assets; исходный ZIP не должен выдавать их наличие за перенос кода.

## 24.3. Текущий результат этого документа

Подготовлен подробный проект миграции, основанный на зафиксированных исходниках и внешней документации выбранных инструментов. Никакие implementation milestones, Rust builds, tests, live smokes или production changes этим документом не объявляются выполненными. Следующий реальный шаг исполнителя — P0 с сохранением inventory и baseline evidence, затем P1 с проверкой наиболее рискованных стыков.

# 25. Источники и ссылки

## 25.1. Исходники agent-run

Все ссылки на файлы ниже закреплены на `c9904f9843ba4a0772bdfa8bac5259f18fad9dc3`, если явно не указан отдельный commit. Дата обращения: 15 сентября 2026 года. Ссылки на группы файлов предназначены также для продолжения аудита; наличие ссылки не означает, что каждый файл группы был построчно проверен.

**R01. База и package metadata.** [pyproject.toml](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/pyproject.toml); [README.md](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/README.md); [Зафиксированный commit](https://github.com/DKotsyuba/agent-run/commit/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3).

**R02. Правила проекта.** [AGENTS.md](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/AGENTS.md).

**R03. Общая архитектура; читать с поправками §2.2.** [docs/architecture.md](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/docs/architecture.md).

**R04. Домен, диспетчер и публичные views.** [domain.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/domain.py); [errors.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/errors.py); [dispatch.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/dispatch.py); [service.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/service.py).

**R05. CLI parser и команды.** [cli.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/cli.py).

**R06. Конфигурация и namespaces.** [config.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/config.py); [native_settings.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/native_settings.py); [accounts.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/accounts.py); [paths.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/paths.py).

**R07. Роли и evidence полномочий.** [profiles.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/profiles.py); [effective_policy.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/effective_policy.py); [role_plan.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/role_plan.py); [service.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/service.py).

**R08. Схема, migrations и store.** [schema.sql](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/state/schema.sql); [migrations.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/state/migrations.py); [store.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/state/store.py).

**R09. Существующий runtime contract.** [docs/runtime-contract.md](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/docs/runtime-contract.md).

**R10. Native continuations.** [docs/continuations.md](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/docs/continuations.md); [resume.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/resume.py).

**R11. Answer proofs и snapshots.** [verify.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/verify.py); [adapters/snapshot_tree.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/adapters/snapshot_tree.py); [adapters/snapshots.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/adapters/snapshots.py).

**R12. Исправление Codex trust/resume в 0.11.15.** [Merge #56](https://github.com/DKotsyuba/agent-run/commit/722ddf45b2ea8aef06c993cb6a136b162d46481b); [codex/snapshot.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/adapters/codex/snapshot.py); [docs/artifact-snapshots.md](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/docs/artifact-snapshots.md).

**R13. Контракт адаптеров и точки входа.** [base.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/adapters/base.py); [codex/adapter.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/adapters/codex/adapter.py); [claude/adapter.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/adapters/claude/adapter.py); [glm/adapter.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/adapters/glm/adapter.py); [qwen/adapter.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/adapters/qwen/adapter.py).

**R14. Socket/MCP контракты.** [docs/api.md](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/docs/api.md); [api_socket.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/api_socket.py); [broker_client.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/broker_client.py); [mcp.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/mcp.py).

**R15. Delivery и signed Desktop relay.** [codex_desktop_host.cjs](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/delivery/codex_desktop_host.cjs); [codex_desktop_relay.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/delivery/codex_desktop_relay.py); [codex_queue.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/delivery/codex_queue.py); [completion_notice_contract.json](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/delivery/completion_notice_contract.json); [dispatch.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/delivery/dispatch.py).

**R16. Capacity ranking и коллекция.** [ranking.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/capacity/ranking.py); [collect.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/capacity/collect.py); [forecast.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/capacity/forecast.py); [snapshot.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/capacity/snapshot.py); [codex_appserver.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/capacity/codex_appserver.py); [OmniRoute reader](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/adapters/omniroute.py).

**R17. Release/deploy contract.** [docs/releasing.md](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/docs/releasing.md); [CI workflow](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/.github/workflows/ci.yml); [Release workflow](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/.github/workflows/release.yml); [scripts/release.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/scripts/release.py); [scripts/release_local.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/scripts/release_local.py).

**R18. Ownership и cleanup semantics.** [docs/process-identity.md](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/docs/process-identity.md); [process_identity.py](https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/process_identity.py).

## 25.2. Первичная документация библиотек и протоколов

Ссылки с `latest` — адреса документации, а не требование ставить плавающую зависимость. Наблюдавшиеся версии указаны в §17; финальный совместимый набор определяет только проверенный Cargo.lock. Дата обращения: 15 сентября 2026 года.

**W01.** [Tokio process: async child I/O и reaping](https://docs.rs/tokio/latest/tokio/process/).

**W02.** [Rusqlite: SQLite bindings и Backup API](https://docs.rs/rusqlite/latest/rusqlite/).

**W03.** [rmcp: официальный Rust MCP SDK и transports](https://docs.rs/rmcp/latest/rmcp/).

**W04.** [Clap: CLI parser](https://docs.rs/clap/latest/clap/).

**W05.** [Rustix: platform/FD wrappers](https://docs.rs/rustix/latest/rustix/).

**W06.** [Reqwest: HTTP client](https://docs.rs/reqwest/latest/reqwest/).

**W07.** [Rust std: Unix CommandExt и ограничения pre_exec](https://doc.rust-lang.org/std/os/unix/process/trait.CommandExt.html).

**W08.** [SQLite Online Backup API](https://sqlite.org/backup.html).

**W09.** [SQLite Write-Ahead Logging](https://sqlite.org/wal.html).

**W10.** [Codex App Server: официальный протокол](https://learn.chatgpt.com/docs/app-server).

**W11.** [Serde: serialization/deserialization](https://docs.rs/serde/latest/serde/).

**W12.** [Serde JSON: JSON representation](https://docs.rs/serde_json/latest/serde_json/).

**W13.** [TOML parser/serializer](https://docs.rs/toml/latest/toml/).

**W14.** [Thiserror: typed errors](https://docs.rs/thiserror/latest/thiserror/).

**W15.** [SHA-2: hashing primitives](https://docs.rs/sha2/latest/sha2/).

**W16.** [Getrandom: random bytes](https://docs.rs/getrandom/latest/getrandom/).

**W17.** [Time: date/time representation](https://docs.rs/time/latest/time/).

**W18.** [Tracing: instrumentation; subscriber отдельно](https://docs.rs/tracing/latest/tracing/); [tracing-subscriber](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/).

**W19.** [Zeroize: limited secret-buffer cleanup](https://docs.rs/zeroize/latest/zeroize/).

**W20.** [Proptest: property-based tests](https://docs.rs/proptest/latest/proptest/).

**W21.** [Insta: snapshot tests](https://docs.rs/insta/latest/insta/).

**W22.** [Tempfile: isolated test files](https://docs.rs/tempfile/latest/tempfile/).

**W23.** [Cargo build: locked/offline semantics](https://doc.rust-lang.org/cargo/commands/cargo-build.html).

**W24.** [Cargo-deny: dependency checks](https://embarkstudios.github.io/cargo-deny/).

**W25.** [Libc: low-level platform FFI](https://docs.rs/libc/latest/libc/).

**W26.** [Plist: launchd configuration serialization](https://docs.rs/plist/latest/plist/).

**W27.** [Tokio-util: async utility components](https://docs.rs/tokio-util/latest/tokio_util/).

**W28.** [Criterion: microbenchmark harness](https://docs.rs/criterion/latest/criterion/).

# Приложение A. Матрица трассируемости

Матрица связывает карту проекта с задачами и испытаниями. Она не заменяет file-level inventory P0; используется как верхний уровень контроля полноты.

| Контракт / исходная область | Этап и целевой владелец | Приёмка |
|---|---|---|
| domain/errors/dispatch/service views | P2, domain + core | T01–T04; G3 |
| config/native_settings/accounts/paths | P2, config + platform | T05–T12; G8 |
| profiles/effective_policy/role_plan | P2, domain + config | T06–T10; A11 |
| state/schema/migrations | P3, store | T13–T17; G4 |
| admission/idempotency/lineage | P3/P5, store + core | T18–T24; G4/G5 |
| launch/supervisor/process identity | P4/P5, platform + core | T25–T36; G5 |
| verify/snapshot_tree/snapshots | P4, platform + artifacts | T37–T48; G4/G8 |
| Codex app-server/permissions/trust | P6, adapters/codex | T48–T54; G6 |
| Claude/GLM/Qwen | P7, соответствующий adapter | T55–T60; G6 |
| CLI/socket/MCP/broker_client | P8, app/transports | T01/T61–T67; G3/G7 |
| delivery/relay/hooks/context | P9, core/delivery + app | T68–T72; G7/G8 |
| capacity/OmniRoute/run_stats | P9, core/capacity + store | T60/T73–T80; G7 |
| init/doctor/operator docs/launchd | P10, app + core/doctor | T81–T83; G9 |
| scripts/release/release_local/CI | P10/P12, xtask | T83/T84; G9/G10 |

# Приложение B. Исполнимый backlog

Все задания ниже имеют исходный статус **planned**. В процессе реализации добавляются commit, ответственный reviewer и evidence path. Для крупных строк исполнитель разбивает работу на подзадачи, не удаляя общий acceptance gate. Порядок основан на зависимостях, а не на предполагаемых календарных сроках.

| ID | Конкретное задание и сохраняемый результат | Зависимости | Проверка |
|---|---|---|---|
| M01 | Checkout pinned baseline; сохранить metadata и hashes | — | SHA/version совпадают с §2 |
| M02 | File/import/resource/test inventory без пропусков | M01 | Coverage manifest для всего дерева |
| M03 | Исходная suite на Mac/Linux; записать failures/skips | M01 | Exit statuses и test IDs сохранены |
| M04 | Sanitized corpus DB/proofs/native frames/config | M02/M03 | Provenance/hash каждого fixture |
| M05 | Workspace/toolchain/lock/CI bootstrap | M01 | Locked check/test на двух ОС |
| M06 | Spawn/READY/reap prototype | M05 | T25/T28/T36 |
| M07 | Birth identity bridge Python↔Rust | M04/M05 | T30/T31 |
| M08 | Canonical JSON compatibility prototype | M04/M05 | T46; A12 |
| M09 | Native Desktop host capability spike | M05 | T71; A09 |
| M10 | MCP SDK negotiation/error/EOF spike | M05 | T67; A14 |
| M11 | Domain newtypes/FSM/errors/view DTO | M05 | T02–T04 |
| M12 | Single tools registry и golden schemas | M11 | T01 |
| M13 | Strict TOML, built-in adapter aliases | M11 | T05/T09/T10 |
| M14 | Paths/accounts/host environment rules | M13 | T11/T12 |
| M15 | Profiles/role resolver/constraints ADR | M13 | T06–T08 |
| M16 | Canonical role/config payload serializer | M08/M15 | T46 |
| M17 | v16 schema creation и owner connections | M11 | T13/T15 |
| M18 | Numbered migrations/backup/locking | M17/M04 | T14/T16/T17 |
| M19 | Admission/replay/concurrency transactions | M15/M17 | T18–T20 |
| M20 | Events/messages/commands и projections | M17 | T23/T24 |
| M21 | Terminal+outbox/evidence atomic operations | M20 | T21/T68/T69 |
| M22 | Lineage/latest-parent uniqueness | M19/M20 | T22 |
| M23 | Anchored no-follow file primitives | M05 | T42/T43 |
| M24 | Answer proof v1/v2 read/seal/verify | M23 | T37–T44 |
| M25 | Managed trees/index/config snapshots | M16/M23 | T45–T47 |
| M26 | Platform process observation и group cleanup | M06/M07 | T29–T34 |
| M27 | Rust fake engine с управляемыми faults | M05 | Fragment/exit/descendant fixtures |
| M28 | Detached supervisor/READY ownership flow | M19/M24/M26/M27 | T25–T29 |
| M29 | Cancel/steer command processing | M20/M28 | T23/T32/T34 |
| M30 | Reconciliation/wait/lifetime semantics | M28/M29 | T30/T31/T35/T36 |
| M31 | Codex app-server session and stream parser | M28 | T49–T52 |
| M32 | Codex permissions/Projects/native settings | M15/M25/M31 | T53/T54 |
| M33 | Codex trust receipt/snapshot finalization | M25/M32 | T48 |
| M34 | Codex accounts/model roster and resume | M22/M31/M33 | T54/T58/T59 |
| M35 | Claude parser/materialization/auth/history | M14/M25/M28 | T55/T58 |
| M36 | GLM adapter specialization | M35 | T56/T58 |
| M37 | Qwen parser/sandbox/macOS Git/history | M14/M25/M28 | T57/T58 |
| M38 | Shared native continuation/identity gate | M22/M34–M37 | T58–T60 |
| M39 | Socket framing/lifetime lock/errors | M12/M19/M30 | T61/T62/T65 |
| M40 | Bounded control/read lanes/long poll/shutdown | M39 | T63/T64/T66 |
| M41 | Full CLI commands, JSON errors and exit codes | M39 | CLI matrix §5.5 |
| M42 | MCP proxy над broker, без local store | M10/M39 | T01/T67 |
| M43 | Capacity sources и scope commit model | M17/M14 | T73/T74/T77/T80 |
| M44 | Forecast/freshness/topology/ranking | M04/M43 | T75/T76/T78/T79 |
| M45 | Run statistics и cumulative usage accounting | M20/M31/M35–M37 | T60 |
| M46 | Delivery claims/backoff/ambiguity/expiry | M21 | T68/T69 |
| M47 | Notice template/safe diagnostics/Unicode escaping | M21/M04 | T69/T70 |
| M48 | Native Desktop relay + Claude UDS transports | M09/M46/M47 | T71 + live UDS |
| M49 | Hook bind/context/receipts and helper modes | M41/M42/M46 | T72 |
| M50 | Init/doctor/docs/resource embedding | M15/M34–M49 | T81/T83 |
| M51 | launchd generators и platform guards | M40/M50 | T82 |
| M52 | Native release/PR/tag/provenance workflow | M05/M50 | Immutable release rehearsal |
| M53 | Deploy journal/quiescence/backup/restore | M18/M51/M52 | T84 |
| M54 | Differential runner и полный contract report | M04/M16/M20/M41–M49 | T01–T84 mapping |
| M55 | Security/fault/load/platform qualification | M24–M54 | G2/G5/G7/G8 |
| M56 | Final source archive, lock, manifest и handoff | M55 | G1–G10 |

# Приложение C. Форма передачи работы следующему исполнителю

Этот шаблон предназначен для файла `migration/status.md`. Пустые поля ниже являются полями будущего отчёта, а не заявлением об уже выполненной реализации.

```text
Baseline Python SHA:
Native implementation SHA:
Toolchain + Cargo.lock hash:
Supported/tested OS and engine versions:

Verified task IDs:
Implemented but not verified:
Blocked tasks and exact reason:
Accepted ADRs and intentional differences:

Last executed command:
Exit status:
Evidence file:
Known failing tests / permitted skips:

Next task ID:
Required inputs:
Expected files and acceptance scenarios:

Production state touched: no / explicitly authorized action
Secrets included in logs or archive: no
```

Передающий работу не пишет «всё готово», если нет evidence, не удаляет failing logs и не оставляет единственные выводы в переписке. Принимающий сначала сверяет файлы и commit, затем выполняет указанную проверку и только после этого продолжает backlog.

# Приложение D. Минимальная программа первого рабочего среза

Первый проверяемый результат после P0/P1 — не все четыре движка, а небольшой end-to-end slice с неизменёнными гарантиями: CLI start → broker → durable starting row → отдельный Rust supervisor → READY → fake engine → message journal → proof v2 → terminal answer. Он должен переживать закрытие CLI и рестарт broker, корректно cancel-иться и отвергать подменённый proof.

Этот срез служит архитектурной проверкой для дальнейшего переноса, но **не называется полной миграцией**. После него каждый новый adapter или operational component подключается к уже проверенному ownership/state/artifact contract, а не строит свою версию запуска и завершения.
