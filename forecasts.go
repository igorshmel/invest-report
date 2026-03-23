package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	pb "github.com/tinkoff/invest-api-go-sdk/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type forecastSyncStats struct {
	ProviderRowsInserted int
	SnapshotsInserted    int
	InstrumentsProcessed int
	InstrumentsSkipped   int
	Warnings             []string
}

type forecastEvalStats struct {
	RowsInserted int
	RowsSkipped  int
	Warnings     []string
}

type forecastInstrumentRef struct {
	InstrumentID string
	Ticker       string
	Name         string
	Currency     string
}

type forecastHorizon struct {
	Code   string
	Months int
}

type providerRecommendationCounts struct {
	StrongBuy  int
	Buy        int
	Hold       int
	Sell       int
	StrongSell int
}

func (c providerRecommendationCounts) total() int {
	return c.StrongBuy + c.Buy + c.Hold + c.Sell + c.StrongSell
}

func (c providerRecommendationCounts) ratingScore() (float64, bool) {
	total := c.total()
	if total <= 0 {
		return 0, false
	}
	sum := 2*c.StrongBuy + c.Buy - c.Sell - 2*c.StrongSell
	return float64(sum) / float64(total), true
}

type providerConsensusData struct {
	Provider          string
	ProviderSymbol    string
	TargetPrice       float64
	TargetAvailable   bool
	TargetCurrency    string
	Recommendation    providerRecommendationCounts
	RatingScore       float64
	RatingAvailable   bool
	ProviderUpdatedAt time.Time
	RawPayloadJSON    string
}

func (d providerConsensusData) hasData() bool {
	return d.TargetAvailable || d.Recommendation.total() > 0 || d.RatingAvailable
}

type forecastProvider interface {
	Name() string
	Fetch(ctx context.Context, symbol string) (providerConsensusData, error)
}

var errNoForecastData = errors.New("forecast data is empty")

func buildForecastProviders(cfg config) ([]forecastProvider, []string) {
	providers := make([]forecastProvider, 0, 2)
	warnings := make([]string, 0, 2)
	addProvider := func(name string) {
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "", "none":
			return
		case "finnhub":
			if strings.TrimSpace(cfg.finnhubAPIKey) == "" {
				warnings = append(warnings, "FORECAST_PROVIDER=finnhub, но FINNHUB_API_KEY пустой: прогнозы пропущены.")
				return
			}
			providers = append(providers, newFinnhubProvider(cfg.finnhubAPIKey, cfg.timeout))
		default:
			warnings = append(warnings, fmt.Sprintf("Неподдерживаемый provider %q: пропущен.", name))
		}
	}

	addProvider(cfg.forecastProvider)
	if cfg.forecastFallbackOn {
		fallback := strings.ToLower(strings.TrimSpace(cfg.forecastFallback))
		if fallback != "" && fallback != strings.ToLower(strings.TrimSpace(cfg.forecastProvider)) {
			addProvider(fallback)
		}
	}

	// de-duplicate by name
	uniq := make(map[string]forecastProvider)
	for _, p := range providers {
		if p == nil {
			continue
		}
		uniq[p.Name()] = p
	}
	providers = providers[:0]
	for _, p := range uniq {
		providers = append(providers, p)
	}
	sort.SliceStable(providers, func(i, j int) bool { return providers[i].Name() < providers[j].Name() })

	return providers, warnings
}

func parseForecastHorizons(raw string) ([]forecastHorizon, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = "1M,3M,6M,12M"
	}
	parts := strings.Split(raw, ",")
	out := make([]forecastHorizon, 0, len(parts))
	seen := map[string]bool{}
	for _, p := range parts {
		p = strings.ToUpper(strings.TrimSpace(p))
		if p == "" {
			continue
		}
		if seen[p] {
			continue
		}
		if !strings.HasSuffix(p, "M") {
			return nil, fmt.Errorf("invalid horizon %q: expected nM", p)
		}
		monthsRaw := strings.TrimSuffix(p, "M")
		months, err := parsePositiveInt(monthsRaw)
		if err != nil {
			return nil, fmt.Errorf("invalid horizon %q: %w", p, err)
		}
		seen[p] = true
		out = append(out, forecastHorizon{Code: p, Months: months})
	}
	if len(out) == 0 {
		return nil, errors.New("empty forecast horizons")
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Months < out[j].Months })
	return out, nil
}

func parsePositiveInt(raw string) (int, error) {
	if raw == "" {
		return 0, errors.New("empty number")
	}
	n := 0
	for _, ch := range raw {
		if ch < '0' || ch > '9' {
			return 0, errors.New("not a number")
		}
		n = n*10 + int(ch-'0')
	}
	if n <= 0 {
		return 0, errors.New("must be > 0")
	}
	return n, nil
}

func syncForecastSnapshots(db *sql.DB, api *apiClients, cfg config, accountID string, now time.Time) (forecastSyncStats, error) {
	stats := forecastSyncStats{}
	providers, providerWarnings := buildForecastProviders(cfg)
	stats.Warnings = append(stats.Warnings, providerWarnings...)
	if len(providers) == 0 {
		return stats, nil
	}
	if api == nil || api.marketData == nil {
		return stats, errors.New("market data client is not initialized")
	}

	instruments, err := loadForecastInstrumentRefs(db, accountID)
	if err != nil {
		return stats, err
	}
	if len(instruments) == 0 {
		stats.Warnings = append(stats.Warnings, "Нет инструментов для sync прогнозов.")
		return stats, nil
	}

	app := &webApp{db: db, api: api, cfg: cfg, accountID: accountID}
	currencies := collectForecastInstrumentCurrencies(instruments)
	fxRates, fxWarnings := app.loadRubFXRates(currencies)
	stats.Warnings = append(stats.Warnings, fxWarnings...)

	snapshotAt := now.UTC().Format(time.RFC3339Nano)
	createdAt := now.UTC().Format(time.RFC3339Nano)

	tx, err := db.Begin()
	if err != nil {
		return stats, err
	}
	defer func() { _ = tx.Rollback() }()

	for _, inst := range instruments {
		stats.InstrumentsProcessed++
		providerRows := make([]providerConsensusData, 0, len(providers))
		providersUsed := make([]string, 0, len(providers))

		for _, p := range providers {
			data, err := fetchProviderConsensusForInstrument(p, inst, cfg.timeout)
			if err != nil {
				if !errors.Is(err, errNoForecastData) {
					stats.Warnings = append(stats.Warnings, fmt.Sprintf("%s %s: %v", strings.ToUpper(inst.Ticker), p.Name(), err))
				}
				continue
			}
			if !data.hasData() {
				continue
			}
			if strings.TrimSpace(data.TargetCurrency) == "" {
				data.TargetCurrency = normalizeCurrency(inst.Currency)
			}
			if err := insertForecastProviderRawTx(tx, accountID, snapshotAt, createdAt, inst, data); err != nil {
				return stats, err
			}
			stats.ProviderRowsInserted++
			providerRows = append(providerRows, data)
			providersUsed = append(providersUsed, data.Provider)
		}

		if len(providerRows) == 0 {
			stats.InstrumentsSkipped++
			continue
		}

		consTarget, targetCurrency, targetOK := aggregateConsensusTarget(providerRows, inst.Currency)
		consScore, scoreOK := aggregateConsensusScore(providerRows)

		priceNano, priceErr := app.fetchLastPriceNanoByInstrumentID(inst.InstrumentID)
		if priceErr != nil {
			stats.Warnings = append(stats.Warnings, fmt.Sprintf("%s: не удалось получить текущую цену: %v", strings.ToUpper(inst.Ticker), priceErr))
		}
		priceCurrency := normalizeCurrency(inst.Currency)
		if priceCurrency == "" && targetCurrency != "" {
			priceCurrency = targetCurrency
		}

		var (
			priceAtSnapshot  sql.NullFloat64
			priceCurrencyDB  sql.NullString
			priceRubNano     sql.NullInt64
			targetPrice      sql.NullFloat64
			targetCurrencyDB sql.NullString
			targetRubNano    sql.NullInt64
			upsidePct        sql.NullFloat64
			ratingScore      sql.NullFloat64
		)

		if priceNano > 0 {
			priceAtSnapshot = sql.NullFloat64{Float64: nanoToFloat(priceNano), Valid: true}
			if priceCurrency != "" {
				priceCurrencyDB = sql.NullString{String: priceCurrency, Valid: true}
			}
			if rub, ok := convertNanoToRUB(priceNano, priceCurrency, fxRates); ok {
				priceRubNano = sql.NullInt64{Int64: rub, Valid: true}
			}
		}

		if targetOK {
			targetPrice = sql.NullFloat64{Float64: consTarget, Valid: true}
			if targetCurrency != "" {
				targetCurrencyDB = sql.NullString{String: targetCurrency, Valid: true}
			}
			if targetNano, ok := floatToNano(consTarget); ok {
				if rub, ok := convertNanoToRUB(targetNano, targetCurrency, fxRates); ok {
					targetRubNano = sql.NullInt64{Int64: rub, Valid: true}
				}
				if priceAtSnapshot.Valid && priceAtSnapshot.Float64 != 0 {
					upsidePct = sql.NullFloat64{Float64: ((consTarget - priceAtSnapshot.Float64) / priceAtSnapshot.Float64) * 100, Valid: true}
				}
			}
		}
		if scoreOK {
			ratingScore = sql.NullFloat64{Float64: consScore, Valid: true}
		}

		if !targetPrice.Valid && !ratingScore.Valid {
			stats.InstrumentsSkipped++
			continue
		}

		if err := insertForecastConsensusSnapshotTx(tx, forecastConsensusInsertRow{
			AccountID:                   accountID,
			SnapshotAt:                  snapshotAt,
			CreatedAt:                   createdAt,
			InstrumentID:                inst.InstrumentID,
			Ticker:                      inst.Ticker,
			Name:                        inst.Name,
			PriceAtSnapshot:             priceAtSnapshot,
			PriceCurrency:               priceCurrencyDB,
			PriceRUBNano:                priceRubNano,
			ConsensusTargetPrice:        targetPrice,
			ConsensusTargetCurrency:     targetCurrencyDB,
			ConsensusTargetPriceRUBNano: targetRubNano,
			ConsensusUpsidePct:          upsidePct,
			ConsensusRatingScore:        ratingScore,
			ProvidersCount:              len(providerRows),
			ProvidersUsed:               strings.Join(uniqueStrings(providersUsed), ","),
		}); err != nil {
			return stats, err
		}
		stats.SnapshotsInserted++
	}

	if err := tx.Commit(); err != nil {
		return stats, err
	}

	return stats, nil
}

func evaluateForecastBacktests(db *sql.DB, api *apiClients, cfg config, accountID string, now time.Time) (forecastEvalStats, error) {
	stats := forecastEvalStats{}
	if api == nil || api.marketData == nil {
		return stats, errors.New("market data client is not initialized")
	}

	horizons, err := parseForecastHorizons(cfg.forecastHorizons)
	if err != nil {
		return stats, err
	}

	app := &webApp{db: db, api: api, cfg: cfg, accountID: accountID}

	for _, h := range horizons {
		pending, err := loadForecastSnapshotsPendingForHorizon(db, accountID, h.Code)
		if err != nil {
			return stats, err
		}
		if len(pending) == 0 {
			continue
		}

		currencies := make([]string, 0, len(pending)*2)
		for _, s := range pending {
			currencies = append(currencies, s.PriceCurrency, s.TargetCurrency)
		}
		fxRates, fxWarnings := app.loadRubFXRates(currencies)
		stats.Warnings = append(stats.Warnings, fxWarnings...)

		cache := map[string]historicalPricePoint{}
		tx, err := db.Begin()
		if err != nil {
			return stats, err
		}

		for _, snap := range pending {
			horizonAt := snap.SnapshotAt.AddDate(0, h.Months, 0)
			if now.UTC().Before(horizonAt) {
				continue
			}

			cacheKey := snap.InstrumentID + "|" + horizonAt.Format("2006-01-02")
			actual, ok := cache[cacheKey]
			if !ok {
				point, found, err := fetchHistoricalCloseAtOrAfter(api, cfg, snap.InstrumentID, horizonAt)
				if err != nil {
					stats.Warnings = append(stats.Warnings, fmt.Sprintf("%s %s: %v", snap.Ticker, h.Code, err))
					stats.RowsSkipped++
					continue
				}
				if !found {
					stats.RowsSkipped++
					continue
				}
				actual = point
				cache[cacheKey] = actual
			}

			actualCurrency := normalizeCurrency(snap.PriceCurrency)
			if actualCurrency == "" {
				actualCurrency = normalizeCurrency(snap.TargetCurrency)
			}

			var actualRubNano sql.NullInt64
			if rub, ok := convertNanoToRUB(actual.PriceNano, actualCurrency, fxRates); ok {
				actualRubNano = sql.NullInt64{Int64: rub, Valid: true}
			}

			signedDiff := actual.Price - snap.TargetPrice
			absDiff := math.Abs(signedDiff)
			errorPct := 0.0
			if snap.TargetPrice != 0 {
				errorPct = (signedDiff / snap.TargetPrice) * 100
			}

			directionPred := signFloat(snap.TargetPrice - snap.PriceAtSnapshot)
			directionActual := signFloat(actual.Price - snap.PriceAtSnapshot)
			directionHit := directionPred != 0 && directionActual != 0 && directionPred == directionActual

			var absRub sql.NullInt64
			if actualRubNano.Valid && snap.TargetPriceRUBNano.Valid {
				d := actualRubNano.Int64 - snap.TargetPriceRUBNano.Int64
				if d < 0 {
					d = -d
				}
				absRub = sql.NullInt64{Int64: d, Valid: true}
			}

			if err := insertForecastBacktestResultTx(tx, forecastBacktestInsertRow{
				AccountID:          accountID,
				SnapshotID:         snap.ID,
				HorizonCode:        h.Code,
				HorizonAt:          horizonAt.UTC().Format(time.RFC3339Nano),
				ActualPrice:        sql.NullFloat64{Float64: actual.Price, Valid: true},
				ActualCurrency:     sql.NullString{String: actualCurrency, Valid: actualCurrency != ""},
				ActualPriceRUBNano: actualRubNano,
				TargetErrorAbs:     sql.NullFloat64{Float64: absDiff, Valid: true},
				TargetErrorAbsRUB:  absRub,
				TargetErrorPct:     sql.NullFloat64{Float64: errorPct, Valid: true},
				DirectionPredicted: directionPred,
				DirectionActual:    directionActual,
				DirectionHit:       directionHit,
				EvaluatedAt:        now.UTC().Format(time.RFC3339Nano),
			}); err != nil {
				_ = tx.Rollback()
				return stats, err
			}
			stats.RowsInserted++
		}

		if err := tx.Commit(); err != nil {
			return stats, err
		}
	}

	return stats, nil
}

type forecastSnapshotPending struct {
	ID                 int64
	InstrumentID       string
	Ticker             string
	SnapshotAt         time.Time
	PriceAtSnapshot    float64
	PriceCurrency      string
	TargetPrice        float64
	TargetCurrency     string
	TargetPriceRUBNano sql.NullInt64
}

func loadForecastSnapshotsPendingForHorizon(db *sql.DB, accountID, horizonCode string) ([]forecastSnapshotPending, error) {
	rows, err := db.Query(`
		SELECT
			s.id,
			s.instrument_id,
			s.ticker,
			s.snapshot_at,
			s.price_at_snapshot,
			COALESCE(s.price_currency, ''),
			s.consensus_target_price,
			COALESCE(s.consensus_target_currency, ''),
			s.consensus_target_price_rub_nano
		FROM forecast_consensus_snapshots s
		WHERE s.account_id = ?
			AND s.consensus_target_price IS NOT NULL
			AND s.price_at_snapshot IS NOT NULL
			AND NOT EXISTS (
				SELECT 1 FROM forecast_backtest_results b
				WHERE b.account_id = s.account_id
					AND b.snapshot_id = s.id
					AND b.horizon_code = ?
			)
		ORDER BY s.snapshot_at, s.id
	`, accountID, horizonCode)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]forecastSnapshotPending, 0)
	for rows.Next() {
		var (
			row         forecastSnapshotPending
			snapshotRaw string
		)
		if err := rows.Scan(
			&row.ID,
			&row.InstrumentID,
			&row.Ticker,
			&snapshotRaw,
			&row.PriceAtSnapshot,
			&row.PriceCurrency,
			&row.TargetPrice,
			&row.TargetCurrency,
			&row.TargetPriceRUBNano,
		); err != nil {
			return nil, err
		}
		row.SnapshotAt, err = time.Parse(time.RFC3339Nano, snapshotRaw)
		if err != nil {
			return nil, fmt.Errorf("parse forecast snapshot_at: %w", err)
		}
		if row.TargetPrice <= 0 || row.PriceAtSnapshot <= 0 {
			continue
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func loadForecastInstrumentRefs(db *sql.DB, accountID string) ([]forecastInstrumentRef, error) {
	rows, err := db.Query(`
		SELECT
			p.instrument_id,
			COALESCE(NULLIF(i.ticker, ''), p.instrument_id) AS ticker,
			COALESCE(NULLIF(i.name, ''), p.instrument_id) AS name,
			COALESCE(NULLIF(i.currency, ''), '') AS currency
		FROM instrument_positions p
		LEFT JOIN instruments i ON i.instrument_id = p.instrument_id
		WHERE p.account_id = ?
		ORDER BY ticker
	`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]forecastInstrumentRef, 0)
	for rows.Next() {
		var row forecastInstrumentRef
		if err := rows.Scan(&row.InstrumentID, &row.Ticker, &row.Name, &row.Currency); err != nil {
			return nil, err
		}
		row.Ticker = strings.ToUpper(strings.TrimSpace(row.Ticker))
		if row.Ticker == "" {
			continue
		}
		row.Currency = normalizeCurrency(row.Currency)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func collectForecastInstrumentCurrencies(rows []forecastInstrumentRef) []string {
	out := make([]string, 0, len(rows))
	seen := map[string]bool{}
	for _, row := range rows {
		code := normalizeCurrency(row.Currency)
		if code == "" || seen[code] {
			continue
		}
		seen[code] = true
		out = append(out, code)
	}
	return out
}

type forecastConsensusInsertRow struct {
	AccountID                   string
	SnapshotAt                  string
	CreatedAt                   string
	InstrumentID                string
	Ticker                      string
	Name                        string
	PriceAtSnapshot             sql.NullFloat64
	PriceCurrency               sql.NullString
	PriceRUBNano                sql.NullInt64
	ConsensusTargetPrice        sql.NullFloat64
	ConsensusTargetCurrency     sql.NullString
	ConsensusTargetPriceRUBNano sql.NullInt64
	ConsensusUpsidePct          sql.NullFloat64
	ConsensusRatingScore        sql.NullFloat64
	ProvidersCount              int
	ProvidersUsed               string
}

func insertForecastConsensusSnapshotTx(tx *sql.Tx, row forecastConsensusInsertRow) error {
	_, err := tx.Exec(`
		INSERT OR IGNORE INTO forecast_consensus_snapshots (
			account_id, snapshot_at, instrument_id, ticker, name,
			price_at_snapshot, price_currency, price_rub_nano,
			consensus_target_price, consensus_target_currency, consensus_target_price_rub_nano,
			consensus_upside_pct, consensus_rating_score,
			providers_count, providers_used, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		row.AccountID,
		row.SnapshotAt,
		row.InstrumentID,
		row.Ticker,
		row.Name,
		row.PriceAtSnapshot,
		row.PriceCurrency,
		row.PriceRUBNano,
		row.ConsensusTargetPrice,
		row.ConsensusTargetCurrency,
		row.ConsensusTargetPriceRUBNano,
		row.ConsensusUpsidePct,
		row.ConsensusRatingScore,
		row.ProvidersCount,
		row.ProvidersUsed,
		row.CreatedAt,
	)
	return err
}

func insertForecastProviderRawTx(tx *sql.Tx, accountID, snapshotAt, createdAt string, inst forecastInstrumentRef, data providerConsensusData) error {
	var (
		target         sql.NullFloat64
		targetCurrency sql.NullString
		rating         sql.NullFloat64
		updatedAt      sql.NullString
	)
	if data.TargetAvailable {
		target = sql.NullFloat64{Float64: data.TargetPrice, Valid: true}
		cur := normalizeCurrency(data.TargetCurrency)
		if cur != "" {
			targetCurrency = sql.NullString{String: cur, Valid: true}
		}
	}
	if data.RatingAvailable {
		rating = sql.NullFloat64{Float64: data.RatingScore, Valid: true}
	}
	if !data.ProviderUpdatedAt.IsZero() {
		updatedAt = sql.NullString{String: data.ProviderUpdatedAt.UTC().Format(time.RFC3339Nano), Valid: true}
	}

	_, err := tx.Exec(`
		INSERT OR IGNORE INTO forecast_provider_raw (
			account_id, snapshot_at, instrument_id, ticker, provider, provider_symbol,
			target_price, target_currency, rating_score,
			buy_cnt, hold_cnt, sell_cnt, strong_buy_cnt, strong_sell_cnt,
			provider_updated_at, payload_json, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		accountID,
		snapshotAt,
		inst.InstrumentID,
		inst.Ticker,
		data.Provider,
		data.ProviderSymbol,
		target,
		targetCurrency,
		rating,
		data.Recommendation.Buy,
		data.Recommendation.Hold,
		data.Recommendation.Sell,
		data.Recommendation.StrongBuy,
		data.Recommendation.StrongSell,
		updatedAt,
		data.RawPayloadJSON,
		createdAt,
	)
	return err
}

type forecastBacktestInsertRow struct {
	AccountID          string
	SnapshotID         int64
	HorizonCode        string
	HorizonAt          string
	ActualPrice        sql.NullFloat64
	ActualCurrency     sql.NullString
	ActualPriceRUBNano sql.NullInt64
	TargetErrorAbs     sql.NullFloat64
	TargetErrorAbsRUB  sql.NullInt64
	TargetErrorPct     sql.NullFloat64
	DirectionPredicted int
	DirectionActual    int
	DirectionHit       bool
	EvaluatedAt        string
}

func insertForecastBacktestResultTx(tx *sql.Tx, row forecastBacktestInsertRow) error {
	dirHit := 0
	if row.DirectionHit {
		dirHit = 1
	}
	_, err := tx.Exec(`
		INSERT OR IGNORE INTO forecast_backtest_results (
			account_id, snapshot_id, horizon_code, horizon_at,
			actual_price, actual_currency, actual_price_rub_nano,
			target_error_abs, target_error_abs_rub_nano, target_error_pct,
			direction_predicted, direction_actual, direction_hit, evaluated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		row.AccountID,
		row.SnapshotID,
		row.HorizonCode,
		row.HorizonAt,
		row.ActualPrice,
		row.ActualCurrency,
		row.ActualPriceRUBNano,
		row.TargetErrorAbs,
		row.TargetErrorAbsRUB,
		row.TargetErrorPct,
		row.DirectionPredicted,
		row.DirectionActual,
		dirHit,
		row.EvaluatedAt,
	)
	return err
}

func aggregateConsensusTarget(rows []providerConsensusData, fallbackCurrency string) (float64, string, bool) {
	total := 0.0
	count := 0
	currency := ""
	for _, row := range rows {
		if !row.TargetAvailable || row.TargetPrice <= 0 {
			continue
		}
		total += row.TargetPrice
		count++
		if currency == "" {
			currency = normalizeCurrency(row.TargetCurrency)
		}
	}
	if count == 0 {
		return 0, "", false
	}
	if currency == "" {
		currency = normalizeCurrency(fallbackCurrency)
	}
	return total / float64(count), currency, true
}

func aggregateConsensusScore(rows []providerConsensusData) (float64, bool) {
	total := 0.0
	count := 0
	for _, row := range rows {
		if !row.RatingAvailable {
			continue
		}
		total += row.RatingScore
		count++
	}
	if count == 0 {
		return 0, false
	}
	return total / float64(count), true
}

func fetchProviderConsensusForInstrument(provider forecastProvider, inst forecastInstrumentRef, timeout time.Duration) (providerConsensusData, error) {
	candidates := forecastSymbolCandidates(inst.Ticker)
	if len(candidates) == 0 {
		return providerConsensusData{}, errNoForecastData
	}
	var lastErr error
	for _, symbol := range candidates {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		data, err := provider.Fetch(ctx, symbol)
		cancel()
		if err == nil && data.hasData() {
			if data.Provider == "" {
				data.Provider = provider.Name()
			}
			if data.ProviderSymbol == "" {
				data.ProviderSymbol = symbol
			}
			if data.TargetCurrency == "" {
				data.TargetCurrency = normalizeCurrency(inst.Currency)
			}
			return data, nil
		}
		if err != nil && !errors.Is(err, errNoForecastData) {
			lastErr = err
		}
	}
	if lastErr != nil {
		return providerConsensusData{}, lastErr
	}
	return providerConsensusData{}, errNoForecastData
}

func forecastSymbolCandidates(ticker string) []string {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	if ticker == "" {
		return nil
	}
	candidates := []string{
		ticker,
		ticker + ".ME",
		"MCX:" + ticker,
		ticker + ":MOEX",
	}
	out := make([]string, 0, len(candidates))
	seen := map[string]bool{}
	for _, s := range candidates {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

type historicalPricePoint struct {
	Price     float64
	PriceNano int64
	Time      time.Time
}

func fetchHistoricalCloseAtOrAfter(api *apiClients, cfg config, instrumentID string, horizonAt time.Time) (historicalPricePoint, bool, error) {
	if api == nil || api.marketData == nil {
		return historicalPricePoint{}, false, errors.New("market data client is not initialized")
	}
	id := strings.TrimSpace(instrumentID)
	if id == "" {
		return historicalPricePoint{}, false, errors.New("empty instrument id")
	}

	start := time.Date(horizonAt.UTC().Year(), horizonAt.UTC().Month(), horizonAt.UTC().Day(), 0, 0, 0, 0, time.UTC)
	afterPoint, err := fetchCandlePoint(api, cfg, id, start, start.AddDate(0, 0, 14), true, start)
	if err != nil {
		return historicalPricePoint{}, false, err
	}
	if afterPoint.PriceNano > 0 {
		return afterPoint, true, nil
	}

	beforePoint, err := fetchCandlePoint(api, cfg, id, start.AddDate(0, 0, -14), start, false, start)
	if err != nil {
		return historicalPricePoint{}, false, err
	}
	if beforePoint.PriceNano > 0 {
		return beforePoint, true, nil
	}

	return historicalPricePoint{}, false, nil
}

func fetchCandlePoint(api *apiClients, cfg config, instrumentID string, from, to time.Time, chooseAfter bool, anchor time.Time) (historicalPricePoint, error) {
	ctx, cancel := requestContext(cfg.appName, cfg.timeout)
	resp, err := api.marketData.GetCandles(ctx, &pb.GetCandlesRequest{
		InstrumentId: instrumentID,
		From:         timestamppb.New(from),
		To:           timestamppb.New(to),
		Interval:     pb.CandleInterval_CANDLE_INTERVAL_DAY,
	})
	cancel()
	if err != nil {
		return historicalPricePoint{}, err
	}

	candles := resp.GetCandles()
	if len(candles) == 0 {
		return historicalPricePoint{}, nil
	}

	if chooseAfter {
		sort.SliceStable(candles, func(i, j int) bool {
			return candles[i].GetTime().AsTime().Before(candles[j].GetTime().AsTime())
		})
		for _, c := range candles {
			ct := c.GetTime().AsTime().UTC()
			if ct.Before(anchor) {
				continue
			}
			priceNano := quotationToNano(c.GetClose())
			if priceNano <= 0 {
				continue
			}
			return historicalPricePoint{Price: nanoToFloat(priceNano), PriceNano: priceNano, Time: ct}, nil
		}
		return historicalPricePoint{}, nil
	}

	sort.SliceStable(candles, func(i, j int) bool {
		return candles[i].GetTime().AsTime().After(candles[j].GetTime().AsTime())
	})
	for _, c := range candles {
		ct := c.GetTime().AsTime().UTC()
		if ct.After(anchor) {
			continue
		}
		priceNano := quotationToNano(c.GetClose())
		if priceNano <= 0 {
			continue
		}
		return historicalPricePoint{Price: nanoToFloat(priceNano), PriceNano: priceNano, Time: ct}, nil
	}
	return historicalPricePoint{}, nil
}

func signFloat(v float64) int {
	const eps = 1e-12
	if v > eps {
		return 1
	}
	if v < -eps {
		return -1
	}
	return 0
}

func floatToNano(v float64) (int64, bool) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, false
	}
	n := v * 1_000_000_000
	if n > float64(math.MaxInt64) || n < float64(math.MinInt64) {
		return 0, false
	}
	return int64(math.Round(n)), true
}

func nanoToFloat(v int64) float64 {
	return float64(v) / 1_000_000_000
}

func uniqueStrings(items []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(items))
	for _, v := range items {
		v = strings.TrimSpace(v)
		if v == "" || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// --- Finnhub provider ---

type finnhubProvider struct {
	apiKey     string
	httpClient *http.Client
}

func newFinnhubProvider(apiKey string, timeout time.Duration) forecastProvider {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return &finnhubProvider{
		apiKey: strings.TrimSpace(apiKey),
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

func (p *finnhubProvider) Name() string { return "finnhub" }

func (p *finnhubProvider) Fetch(ctx context.Context, symbol string) (providerConsensusData, error) {
	symbol = strings.TrimSpace(symbol)
	if symbol == "" {
		return providerConsensusData{}, errNoForecastData
	}
	if strings.TrimSpace(p.apiKey) == "" {
		return providerConsensusData{}, errors.New("FINNHUB_API_KEY is empty")
	}

	target, targetRaw, errTarget := p.fetchPriceTarget(ctx, symbol)
	rec, recRaw, errRec := p.fetchRecommendation(ctx, symbol)

	if errTarget != nil && errRec != nil {
		return providerConsensusData{}, fmt.Errorf("price-target: %v; recommendation: %v", errTarget, errRec)
	}

	payloadObj := map[string]interface{}{
		"price_target":   targetRaw,
		"recommendation": recRaw,
	}
	payloadBytes, _ := json.Marshal(payloadObj)

	out := providerConsensusData{
		Provider:       p.Name(),
		ProviderSymbol: symbol,
		RawPayloadJSON: string(payloadBytes),
	}

	if errTarget == nil && target.TargetMean > 0 {
		out.TargetPrice = target.TargetMean
		out.TargetAvailable = true
		if !target.LastUpdated.IsZero() {
			out.ProviderUpdatedAt = target.LastUpdated
		}
	}
	if errRec == nil && rec.Counts.total() > 0 {
		out.Recommendation = rec.Counts
		if score, ok := rec.Counts.ratingScore(); ok {
			out.RatingScore = score
			out.RatingAvailable = true
		}
		if out.ProviderUpdatedAt.IsZero() || rec.Period.After(out.ProviderUpdatedAt) {
			out.ProviderUpdatedAt = rec.Period
		}
	}

	if !out.hasData() {
		return providerConsensusData{}, errNoForecastData
	}
	return out, nil
}

type finnhubPriceTarget struct {
	Symbol         string    `json:"symbol"`
	TargetHigh     float64   `json:"targetHigh"`
	TargetLow      float64   `json:"targetLow"`
	TargetMean     float64   `json:"targetMean"`
	TargetMedian   float64   `json:"targetMedian"`
	LastUpdatedRaw string    `json:"lastUpdated"`
	LastUpdated    time.Time `json:"-"`
}

type finnhubRecommendationItem struct {
	Symbol     string    `json:"symbol"`
	PeriodRaw  string    `json:"period"`
	Period     time.Time `json:"-"`
	StrongBuy  int       `json:"strongBuy"`
	Buy        int       `json:"buy"`
	Hold       int       `json:"hold"`
	Sell       int       `json:"sell"`
	StrongSell int       `json:"strongSell"`
}

type finnhubRecommendation struct {
	Counts providerRecommendationCounts
	Period time.Time
}

func (p *finnhubProvider) fetchPriceTarget(ctx context.Context, symbol string) (finnhubPriceTarget, map[string]interface{}, error) {
	u := url.URL{Scheme: "https", Host: "finnhub.io", Path: "/api/v1/stock/price-target"}
	q := u.Query()
	q.Set("symbol", symbol)
	q.Set("token", p.apiKey)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return finnhubPriceTarget{}, nil, err
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return finnhubPriceTarget{}, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return finnhubPriceTarget{}, nil, fmt.Errorf("http status %s", resp.Status)
	}

	var payload map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return finnhubPriceTarget{}, nil, err
	}

	bytes, _ := json.Marshal(payload)
	var out finnhubPriceTarget
	if err := json.Unmarshal(bytes, &out); err != nil {
		return finnhubPriceTarget{}, payload, err
	}
	if t := parseFinnhubDate(out.LastUpdatedRaw); !t.IsZero() {
		out.LastUpdated = t
	}
	if out.TargetMean <= 0 {
		return out, payload, errNoForecastData
	}
	return out, payload, nil
}

func (p *finnhubProvider) fetchRecommendation(ctx context.Context, symbol string) (finnhubRecommendation, []map[string]interface{}, error) {
	u := url.URL{Scheme: "https", Host: "finnhub.io", Path: "/api/v1/stock/recommendation"}
	q := u.Query()
	q.Set("symbol", symbol)
	q.Set("token", p.apiKey)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return finnhubRecommendation{}, nil, err
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return finnhubRecommendation{}, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return finnhubRecommendation{}, nil, fmt.Errorf("http status %s", resp.Status)
	}

	var payload []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return finnhubRecommendation{}, nil, err
	}
	if len(payload) == 0 {
		return finnhubRecommendation{}, payload, errNoForecastData
	}

	bytes, _ := json.Marshal(payload)
	var items []finnhubRecommendationItem
	if err := json.Unmarshal(bytes, &items); err != nil {
		return finnhubRecommendation{}, payload, err
	}
	if len(items) == 0 {
		return finnhubRecommendation{}, payload, errNoForecastData
	}
	for i := range items {
		items[i].Period = parseFinnhubDate(items[i].PeriodRaw)
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Period.Equal(items[j].Period) {
			return items[i].PeriodRaw > items[j].PeriodRaw
		}
		return items[i].Period.After(items[j].Period)
	})
	latest := items[0]
	counts := providerRecommendationCounts{
		StrongBuy:  latest.StrongBuy,
		Buy:        latest.Buy,
		Hold:       latest.Hold,
		Sell:       latest.Sell,
		StrongSell: latest.StrongSell,
	}
	if counts.total() == 0 {
		return finnhubRecommendation{}, payload, errNoForecastData
	}

	return finnhubRecommendation{Counts: counts, Period: latest.Period}, payload, nil
}

func parseFinnhubDate(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02", "2006-01"} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}
