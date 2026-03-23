package main

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

type periodBounds struct {
	from time.Time
	to   time.Time
}

type instrumentOverview struct {
	InstrumentID         string
	Ticker               string
	Name                 string
	Currency             string
	Status               string
	OpenQty              int64
	LifetimePNLNano      int64
	PeriodPNLNano        int64
	DealsCount           int64
	PeriodEventsCount    int64
	PeriodPNLAvailable   bool
	LifetimePNLAvailable bool
}

type closedDealRow struct {
	DealID        string
	BuyQty        int64
	BuySumNano    int64
	SellQty       int64
	SellSumNano   int64
	FinalPNLNano  int64
	Currency      string
	OpenTime      time.Time
	CloseTime     time.Time
	BuyOperation  string
	SellOperation string
}

type dividendRow struct {
	OperationID   string
	OperationType string
	AmountNano    int64
	ExecutedAt    time.Time
}

type openDealRow struct {
	OpenDealID   string
	InstrumentID string
	Ticker       string
	Name         string
	Currency     string
	OpenTime     time.Time
	OpenQty      int64
	OpenCostNano int64
	AvgCostNano  int64
	BuyOperation string
}

func openSQLite(path string) (*sql.DB, error) {
	if path == "" {
		return nil, errors.New("empty sqlite path")
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if _, err := db.Exec(`PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;`); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

func initSchema(db *sql.DB) error {
	schema := []string{
		`CREATE TABLE IF NOT EXISTS instruments (
			instrument_id TEXT PRIMARY KEY,
			figi TEXT,
			instrument_uid TEXT,
			ticker TEXT,
			name TEXT,
			currency TEXT,
			updated_at TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS operations_raw (
			operation_id TEXT PRIMARY KEY,
			account_id TEXT NOT NULL,
			instrument_id TEXT,
			figi TEXT,
			instrument_uid TEXT,
			side TEXT NOT NULL,
			operation_type TEXT NOT NULL,
			quantity INTEGER NOT NULL,
			payment_nano INTEGER NOT NULL,
			commission_nano INTEGER NOT NULL,
			price_nano INTEGER NOT NULL,
			currency TEXT,
			executed_at TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			created_at TEXT NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_operations_account_time ON operations_raw(account_id, executed_at);`,
		`CREATE INDEX IF NOT EXISTS idx_operations_instrument_time ON operations_raw(instrument_id, executed_at);`,
		`CREATE TABLE IF NOT EXISTS deals_closed (
			deal_id TEXT PRIMARY KEY,
			account_id TEXT NOT NULL,
			instrument_id TEXT NOT NULL,
			buy_qty INTEGER NOT NULL,
			buy_sum_nano INTEGER NOT NULL,
			sell_qty INTEGER NOT NULL,
			sell_sum_nano INTEGER NOT NULL,
			final_pnl_nano INTEGER NOT NULL,
			currency TEXT NOT NULL,
			open_time TEXT NOT NULL,
			close_time TEXT NOT NULL,
			created_at TEXT NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_deals_account_close_time ON deals_closed(account_id, close_time);`,
		`CREATE INDEX IF NOT EXISTS idx_deals_instrument_close_time ON deals_closed(instrument_id, close_time);`,
		`CREATE TABLE IF NOT EXISTS deal_operation_links (
			deal_id TEXT NOT NULL,
			operation_id TEXT NOT NULL,
			role TEXT NOT NULL,
			allocated_qty INTEGER NOT NULL,
			allocated_nano INTEGER NOT NULL,
			PRIMARY KEY (deal_id, operation_id, role)
		);`,
		`CREATE TABLE IF NOT EXISTS instrument_positions (
			account_id TEXT NOT NULL,
			instrument_id TEXT NOT NULL,
			open_qty INTEGER NOT NULL,
			open_cost_nano INTEGER NOT NULL,
			status TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY (account_id, instrument_id)
		);`,
		`CREATE TABLE IF NOT EXISTS open_deals (
			open_deal_id TEXT PRIMARY KEY,
			account_id TEXT NOT NULL,
			instrument_id TEXT NOT NULL,
			buy_operation_id TEXT NOT NULL,
			open_time TEXT NOT NULL,
			open_qty INTEGER NOT NULL,
			open_cost_nano INTEGER NOT NULL,
			currency TEXT NOT NULL,
			created_at TEXT NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_open_deals_account_instrument ON open_deals(account_id, instrument_id);`,
		`CREATE TABLE IF NOT EXISTS corporate_actions (
			instrument_id TEXT NOT NULL,
			effective_at TEXT NOT NULL,
			action_type TEXT NOT NULL,
			ratio_num INTEGER NOT NULL,
			ratio_den INTEGER NOT NULL,
			comment TEXT,
			PRIMARY KEY (instrument_id, effective_at, action_type)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_corporate_actions_instrument_time ON corporate_actions(instrument_id, effective_at);`,
		`INSERT OR IGNORE INTO corporate_actions (
			instrument_id, effective_at, action_type, ratio_num, ratio_den, comment
		) VALUES (
			'BBG004730ZJ9', '2024-07-15T00:00:00Z', 'SPLIT', 1, 5000, 'VTBR reverse split 1:5000'
		);`,
		`CREATE TABLE IF NOT EXISTS sync_state (
			account_id TEXT PRIMARY KEY,
			last_cursor TEXT,
			last_synced_at TEXT,
			status TEXT NOT NULL,
			error_message TEXT
		);`,
		`CREATE TABLE IF NOT EXISTS forecast_provider_raw (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			account_id TEXT NOT NULL,
			snapshot_at TEXT NOT NULL,
			instrument_id TEXT NOT NULL,
			ticker TEXT NOT NULL,
			provider TEXT NOT NULL,
			provider_symbol TEXT NOT NULL,
			target_price REAL,
			target_currency TEXT,
			rating_score REAL,
			buy_cnt INTEGER NOT NULL DEFAULT 0,
			hold_cnt INTEGER NOT NULL DEFAULT 0,
			sell_cnt INTEGER NOT NULL DEFAULT 0,
			strong_buy_cnt INTEGER NOT NULL DEFAULT 0,
			strong_sell_cnt INTEGER NOT NULL DEFAULT 0,
			provider_updated_at TEXT,
			payload_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			UNIQUE(account_id, snapshot_at, instrument_id, provider)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_forecast_provider_raw_account_time ON forecast_provider_raw(account_id, snapshot_at);`,
		`CREATE TABLE IF NOT EXISTS forecast_consensus_snapshots (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			account_id TEXT NOT NULL,
			snapshot_at TEXT NOT NULL,
			instrument_id TEXT NOT NULL,
			ticker TEXT NOT NULL,
			name TEXT NOT NULL,
			price_at_snapshot REAL,
			price_currency TEXT,
			price_rub_nano INTEGER,
			consensus_target_price REAL,
			consensus_target_currency TEXT,
			consensus_target_price_rub_nano INTEGER,
			consensus_upside_pct REAL,
			consensus_rating_score REAL,
			providers_count INTEGER NOT NULL,
			providers_used TEXT NOT NULL,
			created_at TEXT NOT NULL,
			UNIQUE(account_id, snapshot_at, instrument_id)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_forecast_consensus_account_time ON forecast_consensus_snapshots(account_id, snapshot_at);`,
		`CREATE INDEX IF NOT EXISTS idx_forecast_consensus_account_instrument ON forecast_consensus_snapshots(account_id, instrument_id, snapshot_at);`,
		`CREATE TABLE IF NOT EXISTS forecast_backtest_results (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			account_id TEXT NOT NULL,
			snapshot_id INTEGER NOT NULL,
			horizon_code TEXT NOT NULL,
			horizon_at TEXT NOT NULL,
			actual_price REAL,
			actual_currency TEXT,
			actual_price_rub_nano INTEGER,
			target_error_abs REAL,
			target_error_abs_rub_nano INTEGER,
			target_error_pct REAL,
			direction_predicted INTEGER NOT NULL,
			direction_actual INTEGER NOT NULL,
			direction_hit INTEGER NOT NULL,
			evaluated_at TEXT NOT NULL,
			UNIQUE(account_id, snapshot_id, horizon_code),
			FOREIGN KEY(snapshot_id) REFERENCES forecast_consensus_snapshots(id) ON DELETE CASCADE
		);`,
		`CREATE INDEX IF NOT EXISTS idx_forecast_backtest_account_horizon ON forecast_backtest_results(account_id, horizon_code, evaluated_at);`,
	}

	for _, stmt := range schema {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

func loadInstrumentOverview(db *sql.DB, accountID string, bounds periodBounds) ([]instrumentOverview, error) {
	rows, err := db.Query(`
			SELECT
				p.instrument_id,
				COALESCE(NULLIF(i.ticker, ''), p.instrument_id) AS ticker,
				COALESCE(NULLIF(i.name, ''), p.instrument_id) AS name,
				COALESCE(i.currency, '') AS currency,
				p.status,
				p.open_qty,
				COALESCE(d.lifetime_pnl, 0) + COALESCE(v.lifetime_div, 0) AS lifetime_pnl,
				COALESCE(d.period_pnl, 0) + COALESCE(v.period_div, 0) AS period_pnl,
				COALESCE(d.deals_count, 0) AS deals_count,
				COALESCE(d.period_deals_count, 0) + COALESCE(v.period_div_count, 0) AS period_events_count
			FROM instrument_positions p
			LEFT JOIN instruments i ON i.instrument_id = p.instrument_id
			LEFT JOIN (
				SELECT
					account_id,
					instrument_id,
					SUM(final_pnl_nano) AS lifetime_pnl,
					SUM(CASE WHEN close_time BETWEEN ? AND ? THEN final_pnl_nano ELSE 0 END) AS period_pnl,
					COUNT(*) AS deals_count,
					SUM(CASE WHEN close_time BETWEEN ? AND ? THEN 1 ELSE 0 END) AS period_deals_count
				FROM deals_closed
				WHERE account_id = ?
				GROUP BY account_id, instrument_id
			) d ON d.account_id = p.account_id AND d.instrument_id = p.instrument_id
			LEFT JOIN (
				SELECT
					account_id,
					instrument_id,
					SUM(payment_nano) AS lifetime_div,
					SUM(CASE WHEN executed_at BETWEEN ? AND ? THEN payment_nano ELSE 0 END) AS period_div,
					SUM(CASE WHEN executed_at BETWEEN ? AND ? THEN 1 ELSE 0 END) AS period_div_count
				FROM operations_raw
				WHERE account_id = ?
					AND instrument_id IS NOT NULL
					AND instrument_id <> ''
				AND operation_type IN (
					'OPERATION_TYPE_DIVIDEND',
					'OPERATION_TYPE_DIV_EXT',
					'OPERATION_TYPE_DIVIDEND_TRANSFER'
				)
			GROUP BY account_id, instrument_id
		) v ON v.account_id = p.account_id AND v.instrument_id = p.instrument_id
		WHERE p.account_id = ?
		ORDER BY p.status, COALESCE(NULLIF(i.ticker, ''), p.instrument_id)
		`,
		bounds.from.UTC().Format(time.RFC3339Nano),
		bounds.to.UTC().Format(time.RFC3339Nano),
		bounds.from.UTC().Format(time.RFC3339Nano),
		bounds.to.UTC().Format(time.RFC3339Nano),
		accountID,
		bounds.from.UTC().Format(time.RFC3339Nano),
		bounds.to.UTC().Format(time.RFC3339Nano),
		bounds.from.UTC().Format(time.RFC3339Nano),
		bounds.to.UTC().Format(time.RFC3339Nano),
		accountID,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []instrumentOverview
	for rows.Next() {
		var row instrumentOverview
		if err := rows.Scan(
			&row.InstrumentID,
			&row.Ticker,
			&row.Name,
			&row.Currency,
			&row.Status,
			&row.OpenQty,
			&row.LifetimePNLNano,
			&row.PeriodPNLNano,
			&row.DealsCount,
			&row.PeriodEventsCount,
		); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func loadInstrumentDeals(db *sql.DB, accountID, instrumentID string, bounds periodBounds) ([]closedDealRow, error) {
	rows, err := db.Query(`
		SELECT
			d.deal_id,
			d.buy_qty,
			d.buy_sum_nano,
			d.sell_qty,
			d.sell_sum_nano,
			d.final_pnl_nano,
			d.currency,
			d.open_time,
			d.close_time,
			MAX(CASE WHEN l.role = 'BUY_PART' THEN l.operation_id END) AS buy_operation,
			MAX(CASE WHEN l.role = 'SELL_PART' THEN l.operation_id END) AS sell_operation
		FROM deals_closed d
		LEFT JOIN deal_operation_links l ON l.deal_id = d.deal_id
		WHERE d.account_id = ?
			AND d.instrument_id = ?
			AND d.close_time BETWEEN ? AND ?
		GROUP BY d.deal_id, d.buy_qty, d.buy_sum_nano, d.sell_qty, d.sell_sum_nano, d.final_pnl_nano, d.currency, d.open_time, d.close_time
		ORDER BY d.close_time, d.deal_id
	`, accountID, instrumentID, bounds.from.UTC().Format(time.RFC3339Nano), bounds.to.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]closedDealRow, 0)
	for rows.Next() {
		var row closedDealRow
		var openRaw, closeRaw string
		if err := rows.Scan(
			&row.DealID,
			&row.BuyQty,
			&row.BuySumNano,
			&row.SellQty,
			&row.SellSumNano,
			&row.FinalPNLNano,
			&row.Currency,
			&openRaw,
			&closeRaw,
			&row.BuyOperation,
			&row.SellOperation,
		); err != nil {
			return nil, err
		}

		row.OpenTime, err = time.Parse(time.RFC3339Nano, openRaw)
		if err != nil {
			return nil, fmt.Errorf("parse open_time for deal %s: %w", row.DealID, err)
		}
		row.CloseTime, err = time.Parse(time.RFC3339Nano, closeRaw)
		if err != nil {
			return nil, fmt.Errorf("parse close_time for deal %s: %w", row.DealID, err)
		}

		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func loadInstrumentMeta(db *sql.DB, instrumentID string) (ticker, name, currency string, err error) {
	err = db.QueryRow(`
		SELECT
			COALESCE(NULLIF(ticker, ''), instrument_id),
			COALESCE(NULLIF(name, ''), instrument_id),
			COALESCE(currency, '')
		FROM instruments
		WHERE instrument_id = ?
	`, instrumentID).Scan(&ticker, &name, &currency)
	if errors.Is(err, sql.ErrNoRows) {
		return instrumentID, instrumentID, "", nil
	}
	return ticker, name, currency, err
}

func loadInstrumentLifetimePNL(db *sql.DB, accountID, instrumentID string) (int64, error) {
	var total int64
	err := db.QueryRow(`
		SELECT COALESCE(SUM(final_pnl_nano), 0)
		FROM deals_closed
		WHERE account_id = ?
			AND instrument_id = ?
	`, accountID, instrumentID).Scan(&total)
	return total, err
}

func loadInstrumentDividendTotals(db *sql.DB, accountID, instrumentID string, bounds periodBounds) (periodNano int64, lifetimeNano int64, err error) {
	err = db.QueryRow(`
		SELECT
			COALESCE(SUM(CASE WHEN executed_at BETWEEN ? AND ? THEN payment_nano ELSE 0 END), 0) AS period_div,
			COALESCE(SUM(payment_nano), 0) AS lifetime_div
		FROM operations_raw
		WHERE account_id = ?
			AND instrument_id = ?
			AND operation_type IN (
				'OPERATION_TYPE_DIVIDEND',
				'OPERATION_TYPE_DIV_EXT',
				'OPERATION_TYPE_DIVIDEND_TRANSFER'
			)
	`,
		bounds.from.UTC().Format(time.RFC3339Nano),
		bounds.to.UTC().Format(time.RFC3339Nano),
		accountID,
		instrumentID,
	).Scan(&periodNano, &lifetimeNano)
	return periodNano, lifetimeNano, err
}

func loadInstrumentDividends(db *sql.DB, accountID, instrumentID string, bounds periodBounds) ([]dividendRow, error) {
	rows, err := db.Query(`
		SELECT operation_id, operation_type, payment_nano, executed_at
		FROM operations_raw
		WHERE account_id = ?
			AND instrument_id = ?
			AND executed_at BETWEEN ? AND ?
			AND operation_type IN (
				'OPERATION_TYPE_DIVIDEND',
				'OPERATION_TYPE_DIV_EXT',
				'OPERATION_TYPE_DIVIDEND_TRANSFER'
			)
		ORDER BY executed_at, operation_id
	`,
		accountID,
		instrumentID,
		bounds.from.UTC().Format(time.RFC3339Nano),
		bounds.to.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]dividendRow, 0)
	for rows.Next() {
		var row dividendRow
		var executedRaw string
		if err := rows.Scan(&row.OperationID, &row.OperationType, &row.AmountNano, &executedRaw); err != nil {
			return nil, err
		}
		row.ExecutedAt, err = time.Parse(time.RFC3339Nano, executedRaw)
		if err != nil {
			return nil, fmt.Errorf("parse executed_at for dividend operation %s: %w", row.OperationID, err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func loadOpenDeals(db *sql.DB, accountID string) ([]openDealRow, error) {
	rows, err := db.Query(`
		SELECT
			o.open_deal_id,
			o.instrument_id,
			COALESCE(NULLIF(i.ticker, ''), o.instrument_id) AS ticker,
			COALESCE(NULLIF(i.name, ''), o.instrument_id) AS name,
			COALESCE(NULLIF(o.currency, ''), COALESCE(i.currency, '')) AS currency,
			o.open_time,
			o.open_qty,
			o.open_cost_nano,
			o.buy_operation_id
		FROM open_deals o
		LEFT JOIN instruments i ON i.instrument_id = o.instrument_id
		WHERE o.account_id = ?
		ORDER BY ticker, o.open_time, o.open_deal_id
	`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]openDealRow, 0)
	for rows.Next() {
		var row openDealRow
		var openRaw string
		if err := rows.Scan(
			&row.OpenDealID,
			&row.InstrumentID,
			&row.Ticker,
			&row.Name,
			&row.Currency,
			&openRaw,
			&row.OpenQty,
			&row.OpenCostNano,
			&row.BuyOperation,
		); err != nil {
			return nil, err
		}
		row.OpenTime, err = time.Parse(time.RFC3339Nano, openRaw)
		if err != nil {
			return nil, fmt.Errorf("parse open_time for open deal %s: %w", row.OpenDealID, err)
		}
		if row.OpenQty > 0 {
			row.AvgCostNano = row.OpenCostNano / row.OpenQty
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}
