# T-Invest PnL Analyzer — EARS Specification

## 1. Scope

Документ определяет требования к новой версии приложения для анализа сделок:
- загрузка операций по всем инструментам за всю историю;
- разбор операций в нормализованные "сделки" (closed deals) по инструменту;
- расчет прибыли/убытка за период как суммы финальных результатов сделок;
- хранение в SQLite;
- web UI на Tailwind с разделением на активные и неактивные инструменты.

## 2. Terms

- **Operation**: сырая операция из T-Invest API (`GetOperationsByCursor`).
- **Closed Deal**: запись анализа по инструменту, содержащая:
  - `buy_sum` (и `buy_qty`),
  - `sell_sum` (и `sell_qty`),
  - `final_pnl = sell_sum - buy_sum`.
- **Active Instrument**: инструмент с `open_qty > 0` после обработки всей истории.
- **Inactive Instrument**: инструмент с историей сделок и `open_qty = 0`.
- **Period PnL**: сумма `final_pnl` по сделкам с `close_time` в выбранном периоде.

## 3. Assumptions

- Базовый режим расчета: FIFO.
- В `buy_sum` и `sell_sum` включаются комиссии операции, если доступны в API.
- Мультивалютные результаты не конвертируются автоматически; расчет ведется в валюте сделки.
- Для одной строки `Closed Deal` выполняется `buy_qty == sell_qty`.

## 4. Functional Requirements (EARS)

### 4.1 Data Sync

- **REQ-DS-001 (Ubiquitous)**: The system shall support full-history synchronization of operations for all instruments available to the selected account.
- **REQ-DS-002 (Event-driven)**: When a user starts synchronization, the system shall request operations using cursor-based pagination until no next cursor exists.
- **REQ-DS-003 (Unwanted behavior)**: If the API request fails or times out, the system shall persist sync progress and expose a recoverable error state.
- **REQ-DS-004 (State-driven)**: While synchronization is running, the system shall prevent launching a second sync for the same account.
- **REQ-DS-005 (Ubiquitous)**: The system shall store raw operations idempotently to avoid duplicates across repeated sync runs.

### 4.2 Operation Normalization

- **REQ-ON-001 (Ubiquitous)**: The system shall classify operations by instrument and by side (`BUY`/`SELL`) using official operation types.
- **REQ-ON-002 (Event-driven)**: When an operation has missing `quantity_done`, the system shall fallback to `quantity`.
- **REQ-ON-003 (Unwanted behavior)**: If an operation cannot be mapped to a known instrument id (`figi` or `instrument_uid`), the system shall mark it as unresolved and exclude it from PnL calculation.
- **REQ-ON-004 (Ubiquitous)**: The system shall preserve operation currency and monetary fields with full precision from API payload.

### 4.3 Closed Deal Construction (Core Logic)

- **REQ-CD-001 (Ubiquitous)**: The system shall build closed deals per instrument by matching sells against previously opened buy lots using FIFO.
- **REQ-CD-002 (Event-driven)**: When a sell operation closes one or more buy lots, the system shall create one or more closed deal rows with allocated `buy_sum`, `sell_sum`, and equal quantities.
- **REQ-CD-003 (Ubiquitous)**: The system shall compute `final_pnl` for each closed deal as `sell_sum - buy_sum`.
- **REQ-CD-004 (Event-driven)**: When processing operations in chronological order, the system shall update instrument `open_qty` and `open_cost` after each allocation.
- **REQ-CD-005 (Unwanted behavior)**: If sell quantity exceeds available open quantity, the system shall flag the instrument as data-inconsistent and exclude excess quantity from final PnL until resolved.
- **REQ-CD-006 (Ubiquitous)**: The system shall store `open_time` and `close_time` for each closed deal.

### 4.4 Period PnL

- **REQ-PP-001 (Ubiquitous)**: The system shall calculate user-selected period PnL as the sum of `final_pnl` from closed deals with `close_time` inside the period.
- **REQ-PP-002 (Event-driven)**: When a user changes period filters, the system shall recompute totals from persisted closed deals without re-syncing raw operations.
- **REQ-PP-003 (Ubiquitous)**: The system shall provide period totals per currency and overall grouped by currency.

### 4.5 Instrument Status Buckets

- **REQ-IS-001 (Ubiquitous)**: The system shall maintain two instrument lists: active (`open_qty > 0`) and inactive (`open_qty = 0`).
- **REQ-IS-002 (Event-driven)**: When recalculation changes `open_qty` for an instrument, the system shall move the instrument between active and inactive lists automatically.

### 4.6 UI/UX (Tailwind)

- **REQ-UI-001 (Ubiquitous)**: The system shall provide a main page with separate sections for active and inactive instruments.
- **REQ-UI-002 (Event-driven)**: When a user clicks an instrument in either list, the system shall open an instrument details page.
- **REQ-UI-003 (Ubiquitous)**: The instrument details page shall display closed deals as a table with columns: `buy_sum (buy_qty)`, `sell_sum (sell_qty)`, `final_pnl`, `open_time`, `close_time`, `currency`.
- **REQ-UI-004 (Event-driven)**: When a user selects a period on the details page, the system shall show period PnL by summing `final_pnl` for matching rows.
- **REQ-UI-005 (Optional feature)**: Where the user enables advanced view, the system shall display raw operations linked to a selected closed deal row.
- **REQ-UI-006 (Ubiquitous)**: The system shall be usable on desktop and mobile viewports.

### 4.7 Persistence (SQLite)

- **REQ-DB-001 (Ubiquitous)**: The system shall store data in SQLite.
- **REQ-DB-002 (Ubiquitous)**: The system shall persist at minimum entities for accounts, instruments, raw operations, closed deals, and sync state.
- **REQ-DB-003 (Ubiquitous)**: The system shall enforce unique constraints to guarantee idempotent inserts for raw operations and closed deals.
- **REQ-DB-004 (Event-driven)**: When recalculation is triggered, the system shall recompute closed deals in a transaction and replace previous derived rows atomically.

### 4.8 Auditability and Explainability

- **REQ-AU-001 (Ubiquitous)**: The system shall keep traceability from each closed deal to source operation ids used in allocation.
- **REQ-AU-002 (Event-driven)**: When user opens a closed deal row, the system shall present source operation references.

## 5. Proposed Minimal SQLite Schema

```sql
-- raw instruments
CREATE TABLE instruments (
  instrument_id TEXT PRIMARY KEY,         -- figi or instrument_uid
  figi TEXT,
  instrument_uid TEXT,
  ticker TEXT,
  name TEXT,
  currency TEXT,
  updated_at TEXT NOT NULL
);

-- raw operations (idempotent storage)
CREATE TABLE operations_raw (
  operation_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  instrument_id TEXT,
  side TEXT NOT NULL,                      -- BUY | SELL | OTHER
  operation_type TEXT NOT NULL,
  quantity INTEGER NOT NULL,
  payment_units TEXT NOT NULL,
  commission_units TEXT,
  currency TEXT,
  executed_at TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX idx_operations_account_time ON operations_raw(account_id, executed_at);
CREATE INDEX idx_operations_instrument_time ON operations_raw(instrument_id, executed_at);

-- derived closed deals
CREATE TABLE deals_closed (
  deal_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  buy_qty INTEGER NOT NULL,
  buy_sum_units TEXT NOT NULL,
  sell_qty INTEGER NOT NULL,
  sell_sum_units TEXT NOT NULL,
  final_pnl_units TEXT NOT NULL,
  currency TEXT NOT NULL,
  open_time TEXT NOT NULL,
  close_time TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX idx_deals_account_close_time ON deals_closed(account_id, close_time);
CREATE INDEX idx_deals_instrument_close_time ON deals_closed(instrument_id, close_time);

-- link table for explainability
CREATE TABLE deal_operation_links (
  deal_id TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  role TEXT NOT NULL,                      -- BUY_PART | SELL_PART
  allocated_qty INTEGER NOT NULL,
  allocated_sum_units TEXT NOT NULL,
  PRIMARY KEY (deal_id, operation_id, role)
);

-- per instrument position snapshot
CREATE TABLE instrument_positions (
  account_id TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  open_qty INTEGER NOT NULL,
  open_cost_units TEXT NOT NULL,
  status TEXT NOT NULL,                    -- ACTIVE | INACTIVE
  updated_at TEXT NOT NULL,
  PRIMARY KEY (account_id, instrument_id)
);

CREATE TABLE sync_state (
  account_id TEXT PRIMARY KEY,
  last_cursor TEXT,
  last_synced_at TEXT,
  status TEXT NOT NULL,                    -- IDLE | RUNNING | FAILED
  error_message TEXT
);
```

## 6. Acceptance Criteria (High-Level)

- Пользователь видит 2 списка инструментов: активные и неактивные.
- При клике по инструменту открывается таблица сделок формата:
  - `buy_sum (buy_qty)`
  - `sell_sum (sell_qty)`
  - `final_pnl`
- PnL за период считается как сумма `final_pnl` по таблице сделок за период.
- Перезапуск синхронизации не создает дубликаты.
- Пересчет сделок воспроизводим и дает одинаковый результат на одинаковом наборе операций.

## 7. Out of Scope (for first increment)

- Автоматическая FX-конвертация в единую валюту.
- Налоговый учет (НДФЛ) как отдельный модуль.
- Поддержка деривативов и сложных инструментов вне базовой модели BUY/SELL.
