# tinvest-pnl-report

Приложение для анализа сделок T-Invest.

Поддерживает:
- синхронизацию полной истории операций в SQLite;
- перерасчет закрытых сделок по FIFO;
- расчет `final_pnl = sell_sum - buy_sum` по каждой закрытой сделке;
- учет дивидендов в `Period PnL` и `Lifetime PnL`;
- web UI (Tailwind): `ACTIVE/INACTIVE` инструменты + детализация сделок + вкладка `Open Deals`;
- аналитику внешних прогнозов аналитиков: snapshot-консенсус + backtest качества.

## Настройка один раз

1. Создайте и заполните конфиг:

```bash
cd /home/shmel/go/src/tinvest-pnl-report
cp config.env.example config.env
```

2. В `config.env` заполните минимум:

```bash
TINVEST_TOKEN="..."
TINVEST_ACCOUNT_ID="..."
```

Для подсистемы прогнозов заполните (опционально, но рекомендуется):

```bash
FORECAST_PROVIDER_PRIMARY="finnhub"
FINNHUB_API_KEY="..."
FORECAST_HORIZONS="1M,3M,6M,12M"
```

После этого токен и account id вводить в командах не нужно.

## Основные команды (Makefile)

```bash
make help
make check-config
make list-accounts
make sync
make sync-forecasts
make eval-forecasts
make sync-all
make serve
make run
```

- `make sync`: синхронизация + перерасчет сделок в SQLite.
- `make sync-forecasts`: загрузка snapshot прогнозов аналитиков в SQLite.
- `make eval-forecasts`: ретроспективная оценка созревших прогнозов (1M/3M/6M/12M).
- `make sync-all`: `sync + sync-forecasts + eval-forecasts`.
- `make serve`: запуск UI.
- `make run`: `sync-all -> serve` и попытка открыть браузер автоматически.

## run.sh

`run.sh` поддерживает режимы:

```bash
./run.sh check-config
./run.sh list-accounts
./run.sh sync
./run.sh sync-forecasts
./run.sh eval-forecasts
./run.sh sync-all
./run.sh serve
./run.sh report
./run.sh run
```

Где `./run.sh run` делает:
1. синхронизацию,
2. запуск web UI,
3. автооткрытие браузера на `http://localhost:<port>`.

## Режимы приложения

- `-mode report`: старый прямой отчет из API (без SQLite/UI).
- `-mode sync`: синк + перерасчет в SQLite.
- `-mode sync-forecasts`: загрузка консенсус-прогнозов аналитиков.
- `-mode evaluate-forecasts`: backtest качества прогнозов по горизонтам.
- `-mode sync-all`: `sync + sync-forecasts + evaluate-forecasts`.
- `-mode serve`: web UI.

## Что показывает UI

- Главная:
  - `Active Instruments` (`open_qty > 0`)
  - `Blocked Instruments` (тикеры `FIXR`, `POGR`)
  - `Inactive Instruments` (`open_qty = 0`)
  - отдельные блоки `USD (Historical / Not Tradable)` и `EUR (Historical / Not Tradable)`
  - списки отсортированы по PnL: сверху максимальный плюс, снизу максимальный минус
- Вкладка `Open Deals`:
  - сводка открытых позиций по инструментам (`Open Buys`/`Open Qty`/Broker avg/current price/current pnl)
  - отдельные списки `RUB/Other`, `USD`, `EUR`
  - сортировка списков по `Current PnL (Broker, RUB)`: сверху максимальный плюс, снизу максимальный минус
- Вкладка `Forecasts`:
  - latest snapshot консенсуса по инструментам: текущая цена, target, upside, score, providers
- Вкладка `Forecast Quality`:
  - агрегаты backtest по горизонтам: `MAE (RUB)`, `MAPE (%)`, `Direction Hit (%)`
- Деталка `/forecast/<instrument_id>`:
  - история snapshot’ов и список результатов backtest
- Деталка инструмента:
  - `Buy Sum (Qty)`
  - `Sell Sum (Qty)`
  - `Final PnL`
  - таблица дивидендов за период
  - `Period PnL` как `сделки + дивиденды` за выбранный период

## TLS / сертификаты

В `config.env`:
- `TINVEST_CA_CERT_FILE` — путь к доверенному CA (рекомендуется).
- `TINVEST_INSECURE_SKIP_VERIFY="true"` — временный небезопасный обход.

## Точность и ограничения

- Расчет ведется по операциям покупки/продажи акций.
- PnL по строкам инструмента хранится в валюте инструмента.
- `ИТОГО` в таблицах `Active/Inactive` и `Open Deals` конвертируется в RUB по актуальному FX-курсу из API.
- В пересчете FIFO поддерживаются корпоративные сплиты через таблицу `corporate_actions` (включено правило для VTBR reverse split 1:5000).
- Если продаж больше, чем доступных лотов покупки, избыточная часть исключается из PnL и фиксируется в `sync_state.error_message`.
- Прогнозы аналитиков берутся из внешнего provider (`FORECAST_PROVIDER_PRIMARY`, по умолчанию `finnhub`).
- Для ретро-оценки прогнозов используется дневная свеча T-Invest на дату горизонта или ближайшую доступную после (fallback: ближайшая до).
- История прогнозов immutable: snapshot’ы только добавляются, старые записи не перезаписываются.

## Спецификация

- EARS-спека: [`SPEC_EARS.md`](./SPEC_EARS.md)
