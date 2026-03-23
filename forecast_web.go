package main

import (
	"database/sql"
	"fmt"
	"html/template"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

type forecastsPageData struct {
	AccountID string
	From      string
	To        string
	Rows      []forecastOverviewRow
	Warning   string
	Error     string
}

type forecastOverviewRow struct {
	InstrumentID   string
	Ticker         string
	Name           string
	SnapshotAt     time.Time
	PriceNano      int64
	PriceCurrency  string
	TargetNano     int64
	TargetCurrency string
	UpsidePct      float64
	UpsideKnown    bool
	RatingScore    float64
	RatingKnown    bool
	ProvidersCount int
	ProvidersUsed  string
}

type forecastQualityPageData struct {
	AccountID string
	From      string
	To        string
	Rows      []forecastQualityRow
	Error     string
}

type forecastQualityRow struct {
	HorizonCode     string
	Samples         int
	MAERUBNano      int64
	MAPEPercent     float64
	DirectionHitPct float64
}

type forecastDetailsPageData struct {
	AccountID    string
	InstrumentID string
	Ticker       string
	Name         string
	From         string
	To           string
	Snapshots    []forecastSnapshotHistoryRow
	Backtests    []forecastBacktestHistoryRow
	Error        string
}

type forecastSnapshotHistoryRow struct {
	SnapshotID     int64
	SnapshotAt     time.Time
	PriceNano      int64
	PriceCurrency  string
	TargetNano     int64
	TargetCurrency string
	UpsidePct      float64
	UpsideKnown    bool
	RatingScore    float64
	RatingKnown    bool
	ProvidersCount int
	ProvidersUsed  string
}

type forecastBacktestHistoryRow struct {
	SnapshotAt      time.Time
	HorizonCode     string
	HorizonAt       time.Time
	ActualPriceNano int64
	ActualCurrency  string
	ErrorPct        float64
	ErrorPctKnown   bool
	ErrorRUBNano    int64
	ErrorRUBKnown   bool
	DirectionHit    bool
}

var forecastsTemplate = template.Must(template.New("forecasts").Funcs(tplFuncMap).Parse(`<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Forecasts Consensus</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-100 text-slate-900">
  <main class="max-w-7xl mx-auto p-4 md:p-8 space-y-6">
    <header class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
      <h1 class="text-2xl md:text-3xl font-semibold">Forecasts / Consensus</h1>
      <p class="text-sm text-slate-600 mt-1">Account: <span class="font-mono">{{.AccountID}}</span></p>
      <div class="mt-3 text-sm">
        <a href="/?from={{.From}}&to={{.To}}" class="text-blue-700 hover:underline mr-3">Instruments</a>
        <a href="/open-deals" class="text-blue-700 hover:underline mr-3">Open Deals</a>
        <a href="/moex-bluechips?from={{.From}}&to={{.To}}" class="text-blue-700 hover:underline mr-3">MOEX Blue Chips</a>
        <a href="/forecasts?from={{.From}}&to={{.To}}" class="text-blue-700 hover:underline mr-3">Forecasts</a>
        <a href="/forecast-quality?from={{.From}}&to={{.To}}" class="text-blue-700 hover:underline">Forecast Quality</a>
      </div>
    </header>

    {{if .Warning}}
    <div class="rounded-xl bg-amber-50 text-amber-900 border border-amber-200 p-3 text-sm">{{.Warning}}</div>
    {{end}}
    {{if .Error}}
    <div class="rounded-xl bg-rose-50 text-rose-800 border border-rose-200 p-3 text-sm">{{.Error}}</div>
    {{end}}

    <section class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
      <form method="get" action="/forecasts" class="grid grid-cols-1 md:grid-cols-5 gap-3 items-end">
        <label class="block text-sm">
          <span class="text-slate-600">From</span>
          <input type="date" name="from" value="{{.From}}" class="mt-1 w-full rounded-lg border-slate-300" />
        </label>
        <label class="block text-sm">
          <span class="text-slate-600">To</span>
          <input type="date" name="to" value="{{.To}}" class="mt-1 w-full rounded-lg border-slate-300" />
        </label>
        <button class="h-10 rounded-lg bg-slate-900 text-white px-4">Apply Period</button>
      </form>
    </section>

    <section class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
      <h2 class="text-xl font-semibold mb-3">Latest Consensus by Instrument</h2>
      <div class="overflow-x-auto">
        <table class="min-w-full text-sm">
          <thead class="text-slate-500 text-left">
            <tr>
              <th class="py-2 pr-4">Instrument</th>
              <th class="py-2 pr-4">Snapshot</th>
              <th class="py-2 pr-4">Current Price</th>
              <th class="py-2 pr-4">Consensus Target</th>
              <th class="py-2 pr-4">Upside %</th>
              <th class="py-2 pr-4">Consensus Score</th>
              <th class="py-2 pr-4">Providers</th>
            </tr>
          </thead>
          <tbody>
            {{if .Rows}}
              {{range .Rows}}
              <tr class="border-t border-slate-100">
                <td class="py-2 pr-4">
                  <a class="text-blue-700 hover:underline font-medium" href="/forecast/{{pathEscape .InstrumentID}}?from={{$.From}}&to={{$.To}}">{{.Ticker}}</a>
                  <div class="text-xs text-slate-500">{{.Name}}</div>
                </td>
                <td class="py-2 pr-4 font-mono text-xs">{{formatTime .SnapshotAt}}</td>
                <td class="py-2 pr-4 font-mono">{{formatMoney .PriceNano}} <span class="text-xs text-slate-500">{{.PriceCurrency}}</span></td>
                <td class="py-2 pr-4 font-mono">{{formatMoney .TargetNano}} <span class="text-xs text-slate-500">{{.TargetCurrency}}</span></td>
                {{if .UpsideKnown}}
                <td class="py-2 pr-4 font-mono">{{printf "%.2f%%" .UpsidePct}}</td>
                {{else}}
                <td class="py-2 pr-4 text-slate-500">N/A</td>
                {{end}}
                {{if .RatingKnown}}
                <td class="py-2 pr-4 font-mono">{{printf "%.3f" .RatingScore}}</td>
                {{else}}
                <td class="py-2 pr-4 text-slate-500">N/A</td>
                {{end}}
                <td class="py-2 pr-4 font-mono text-xs">{{.ProvidersCount}} · {{.ProvidersUsed}}</td>
              </tr>
              {{end}}
            {{else}}
              <tr><td colspan="7" class="py-3 text-slate-500">No forecast snapshots for selected period.</td></tr>
            {{end}}
          </tbody>
        </table>
      </div>
    </section>
  </main>
</body>
</html>`))

var forecastQualityTemplate = template.Must(template.New("forecastQuality").Funcs(tplFuncMap).Parse(`<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Forecast Quality</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-100 text-slate-900">
  <main class="max-w-7xl mx-auto p-4 md:p-8 space-y-6">
    <header class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
      <h1 class="text-2xl md:text-3xl font-semibold">Forecast Quality</h1>
      <p class="text-sm text-slate-600 mt-1">Account: <span class="font-mono">{{.AccountID}}</span></p>
      <div class="mt-3 text-sm">
        <a href="/?from={{.From}}&to={{.To}}" class="text-blue-700 hover:underline mr-3">Instruments</a>
        <a href="/forecasts?from={{.From}}&to={{.To}}" class="text-blue-700 hover:underline mr-3">Forecasts</a>
        <a href="/forecast-quality?from={{.From}}&to={{.To}}" class="text-blue-700 hover:underline">Forecast Quality</a>
      </div>
    </header>

    {{if .Error}}
    <div class="rounded-xl bg-rose-50 text-rose-800 border border-rose-200 p-3 text-sm">{{.Error}}</div>
    {{end}}

    <section class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
      <form method="get" action="/forecast-quality" class="grid grid-cols-1 md:grid-cols-5 gap-3 items-end">
        <label class="block text-sm">
          <span class="text-slate-600">From</span>
          <input type="date" name="from" value="{{.From}}" class="mt-1 w-full rounded-lg border-slate-300" />
        </label>
        <label class="block text-sm">
          <span class="text-slate-600">To</span>
          <input type="date" name="to" value="{{.To}}" class="mt-1 w-full rounded-lg border-slate-300" />
        </label>
        <button class="h-10 rounded-lg bg-slate-900 text-white px-4">Apply Period</button>
      </form>
    </section>

    <section class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
      <h2 class="text-xl font-semibold mb-3">Backtest Metrics by Horizon</h2>
      <div class="overflow-x-auto">
        <table class="min-w-full text-sm">
          <thead class="text-slate-500 text-left">
            <tr>
              <th class="py-2 pr-4">Horizon</th>
              <th class="py-2 pr-4">Samples</th>
              <th class="py-2 pr-4">MAE (RUB)</th>
              <th class="py-2 pr-4">MAPE (%)</th>
              <th class="py-2 pr-4">Direction Hit (%)</th>
            </tr>
          </thead>
          <tbody>
            {{if .Rows}}
              {{range .Rows}}
              <tr class="border-t border-slate-100">
                <td class="py-2 pr-4 font-mono">{{.HorizonCode}}</td>
                <td class="py-2 pr-4 font-mono">{{.Samples}}</td>
                <td class="py-2 pr-4 font-mono">{{formatMoney .MAERUBNano}}</td>
                <td class="py-2 pr-4 font-mono">{{printf "%.2f%%" .MAPEPercent}}</td>
                <td class="py-2 pr-4 font-mono">{{printf "%.2f%%" .DirectionHitPct}}</td>
              </tr>
              {{end}}
            {{else}}
              <tr><td colspan="5" class="py-3 text-slate-500">No backtest results for selected period.</td></tr>
            {{end}}
          </tbody>
        </table>
      </div>
    </section>
  </main>
</body>
</html>`))

var forecastDetailsTemplate = template.Must(template.New("forecastDetails").Funcs(tplFuncMap).Parse(`<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Forecast Details</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-100 text-slate-900">
  <main class="max-w-7xl mx-auto p-4 md:p-8 space-y-6">
    <header class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
      <a href="/forecasts?from={{.From}}&to={{.To}}" class="text-sm text-blue-700 hover:underline">← Back to forecasts</a>
      <h1 class="text-2xl font-semibold mt-2">{{.Ticker}}</h1>
      <p class="text-sm text-slate-600">{{.Name}}</p>
      <p class="text-xs text-slate-500 mt-1">Instrument ID: <span class="font-mono">{{.InstrumentID}}</span></p>
    </header>

    {{if .Error}}
    <div class="rounded-xl bg-rose-50 text-rose-800 border border-rose-200 p-3 text-sm">{{.Error}}</div>
    {{end}}

    <section class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
      <form method="get" class="grid grid-cols-1 md:grid-cols-5 gap-3 items-end">
        <label class="block text-sm">
          <span class="text-slate-600">From</span>
          <input type="date" name="from" value="{{.From}}" class="mt-1 w-full rounded-lg border-slate-300" />
        </label>
        <label class="block text-sm">
          <span class="text-slate-600">To</span>
          <input type="date" name="to" value="{{.To}}" class="mt-1 w-full rounded-lg border-slate-300" />
        </label>
        <button class="h-10 rounded-lg bg-slate-900 text-white px-4">Apply Period</button>
      </form>
    </section>

    <section class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
      <h2 class="text-xl font-semibold mb-3">Consensus Snapshots</h2>
      <div class="overflow-x-auto">
        <table class="min-w-full text-sm">
          <thead class="text-slate-500 text-left">
            <tr>
              <th class="py-2 pr-4">Snapshot</th>
              <th class="py-2 pr-4">Current Price</th>
              <th class="py-2 pr-4">Target</th>
              <th class="py-2 pr-4">Upside %</th>
              <th class="py-2 pr-4">Score</th>
              <th class="py-2 pr-4">Providers</th>
            </tr>
          </thead>
          <tbody>
            {{if .Snapshots}}
              {{range .Snapshots}}
              <tr class="border-t border-slate-100">
                <td class="py-2 pr-4 font-mono text-xs">{{formatTime .SnapshotAt}}</td>
                <td class="py-2 pr-4 font-mono">{{formatMoney .PriceNano}} <span class="text-xs text-slate-500">{{.PriceCurrency}}</span></td>
                <td class="py-2 pr-4 font-mono">{{formatMoney .TargetNano}} <span class="text-xs text-slate-500">{{.TargetCurrency}}</span></td>
                {{if .UpsideKnown}}
                <td class="py-2 pr-4 font-mono">{{printf "%.2f%%" .UpsidePct}}</td>
                {{else}}
                <td class="py-2 pr-4 text-slate-500">N/A</td>
                {{end}}
                {{if .RatingKnown}}
                <td class="py-2 pr-4 font-mono">{{printf "%.3f" .RatingScore}}</td>
                {{else}}
                <td class="py-2 pr-4 text-slate-500">N/A</td>
                {{end}}
                <td class="py-2 pr-4 font-mono text-xs">{{.ProvidersCount}} · {{.ProvidersUsed}}</td>
              </tr>
              {{end}}
            {{else}}
              <tr><td colspan="6" class="py-3 text-slate-500">No consensus snapshots for selected period.</td></tr>
            {{end}}
          </tbody>
        </table>
      </div>
    </section>

    <section class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
      <h2 class="text-xl font-semibold mb-3">Backtest Results</h2>
      <div class="overflow-x-auto">
        <table class="min-w-full text-sm">
          <thead class="text-slate-500 text-left">
            <tr>
              <th class="py-2 pr-4">Snapshot</th>
              <th class="py-2 pr-4">Horizon</th>
              <th class="py-2 pr-4">Horizon At</th>
              <th class="py-2 pr-4">Actual Price</th>
              <th class="py-2 pr-4">Error %</th>
              <th class="py-2 pr-4">Error RUB</th>
              <th class="py-2 pr-4">Direction Hit</th>
            </tr>
          </thead>
          <tbody>
            {{if .Backtests}}
              {{range .Backtests}}
              <tr class="border-t border-slate-100">
                <td class="py-2 pr-4 font-mono text-xs">{{formatTime .SnapshotAt}}</td>
                <td class="py-2 pr-4 font-mono">{{.HorizonCode}}</td>
                <td class="py-2 pr-4 font-mono text-xs">{{formatTime .HorizonAt}}</td>
                <td class="py-2 pr-4 font-mono">{{formatMoney .ActualPriceNano}} <span class="text-xs text-slate-500">{{.ActualCurrency}}</span></td>
                {{if .ErrorPctKnown}}
                <td class="py-2 pr-4 font-mono">{{printf "%.2f%%" .ErrorPct}}</td>
                {{else}}
                <td class="py-2 pr-4 text-slate-500">N/A</td>
                {{end}}
                {{if .ErrorRUBKnown}}
                <td class="py-2 pr-4 font-mono">{{formatMoney .ErrorRUBNano}}</td>
                {{else}}
                <td class="py-2 pr-4 text-slate-500">N/A</td>
                {{end}}
                <td class="py-2 pr-4">{{if .DirectionHit}}Yes{{else}}No{{end}}</td>
              </tr>
              {{end}}
            {{else}}
              <tr><td colspan="7" class="py-3 text-slate-500">No backtest results for selected period.</td></tr>
            {{end}}
          </tbody>
        </table>
      </div>
    </section>
  </main>
</body>
</html>`))

func (a *webApp) handleForecasts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	fromRaw := strings.TrimSpace(r.URL.Query().Get("from"))
	toRaw := strings.TrimSpace(r.URL.Query().Get("to"))
	bounds, from, to, parseErr := parseWebBounds(fromRaw, toRaw)

	data := forecastsPageData{AccountID: a.accountID, From: from, To: to}
	if parseErr != nil {
		data.Error = parseErr.Error()
		_ = forecastsTemplate.Execute(w, data)
		return
	}

	rows, err := loadForecastLatestRows(a.db, a.accountID, bounds)
	if err != nil {
		data.Error = err.Error()
		_ = forecastsTemplate.Execute(w, data)
		return
	}
	data.Rows = rows
	_ = forecastsTemplate.Execute(w, data)
}

func (a *webApp) handleForecastQuality(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	fromRaw := strings.TrimSpace(r.URL.Query().Get("from"))
	toRaw := strings.TrimSpace(r.URL.Query().Get("to"))
	bounds, from, to, parseErr := parseWebBounds(fromRaw, toRaw)

	data := forecastQualityPageData{AccountID: a.accountID, From: from, To: to}
	if parseErr != nil {
		data.Error = parseErr.Error()
		_ = forecastQualityTemplate.Execute(w, data)
		return
	}

	rows, err := loadForecastQualityRows(a.db, a.accountID, bounds)
	if err != nil {
		data.Error = err.Error()
		_ = forecastQualityTemplate.Execute(w, data)
		return
	}
	data.Rows = rows
	_ = forecastQualityTemplate.Execute(w, data)
}

func (a *webApp) handleForecastInstrument(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	instrumentID, err := url.PathUnescape(strings.TrimPrefix(r.URL.Path, "/forecast/"))
	if err != nil || strings.TrimSpace(instrumentID) == "" {
		http.NotFound(w, r)
		return
	}

	fromRaw := strings.TrimSpace(r.URL.Query().Get("from"))
	toRaw := strings.TrimSpace(r.URL.Query().Get("to"))
	bounds, from, to, parseErr := parseWebBounds(fromRaw, toRaw)

	ticker, name, _, metaErr := loadInstrumentMeta(a.db, instrumentID)
	data := forecastDetailsPageData{
		AccountID:    a.accountID,
		InstrumentID: instrumentID,
		Ticker:       ticker,
		Name:         name,
		From:         from,
		To:           to,
	}
	if parseErr != nil {
		data.Error = parseErr.Error()
		_ = forecastDetailsTemplate.Execute(w, data)
		return
	}
	if metaErr != nil {
		data.Error = metaErr.Error()
		_ = forecastDetailsTemplate.Execute(w, data)
		return
	}

	snapshots, err := loadForecastSnapshotHistory(a.db, a.accountID, instrumentID, bounds)
	if err != nil {
		data.Error = err.Error()
		_ = forecastDetailsTemplate.Execute(w, data)
		return
	}
	data.Snapshots = snapshots

	backtests, err := loadForecastBacktestHistory(a.db, a.accountID, instrumentID, bounds)
	if err != nil {
		data.Error = err.Error()
		_ = forecastDetailsTemplate.Execute(w, data)
		return
	}
	data.Backtests = backtests

	_ = forecastDetailsTemplate.Execute(w, data)
}

func loadForecastLatestRows(db *sql.DB, accountID string, bounds periodBounds) ([]forecastOverviewRow, error) {
	rows, err := db.Query(`
		WITH latest AS (
			SELECT instrument_id, MAX(snapshot_at) AS snapshot_at
			FROM forecast_consensus_snapshots
			WHERE account_id = ?
				AND snapshot_at BETWEEN ? AND ?
			GROUP BY instrument_id
		)
		SELECT
			s.instrument_id,
			s.ticker,
			s.name,
			s.snapshot_at,
			s.price_at_snapshot,
			COALESCE(s.price_currency, ''),
			s.consensus_target_price,
			COALESCE(s.consensus_target_currency, ''),
			s.consensus_upside_pct,
			s.consensus_rating_score,
			s.providers_count,
			COALESCE(s.providers_used, '')
		FROM forecast_consensus_snapshots s
		JOIN latest l ON l.instrument_id = s.instrument_id AND l.snapshot_at = s.snapshot_at
		WHERE s.account_id = ?
		ORDER BY COALESCE(s.consensus_upside_pct, -1e18) DESC, s.ticker
	`,
		accountID,
		bounds.from.UTC().Format(time.RFC3339Nano),
		bounds.to.UTC().Format(time.RFC3339Nano),
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]forecastOverviewRow, 0)
	for rows.Next() {
		var (
			row         forecastOverviewRow
			snapshotRaw string
			price       sql.NullFloat64
			target      sql.NullFloat64
			upside      sql.NullFloat64
			rating      sql.NullFloat64
		)
		if err := rows.Scan(
			&row.InstrumentID,
			&row.Ticker,
			&row.Name,
			&snapshotRaw,
			&price,
			&row.PriceCurrency,
			&target,
			&row.TargetCurrency,
			&upside,
			&rating,
			&row.ProvidersCount,
			&row.ProvidersUsed,
		); err != nil {
			return nil, err
		}
		row.SnapshotAt, err = time.Parse(time.RFC3339Nano, snapshotRaw)
		if err != nil {
			return nil, fmt.Errorf("parse forecast snapshot time: %w", err)
		}
		if price.Valid {
			if n, ok := floatToNano(price.Float64); ok {
				row.PriceNano = n
			}
		}
		if target.Valid {
			if n, ok := floatToNano(target.Float64); ok {
				row.TargetNano = n
			}
		}
		if upside.Valid {
			row.UpsidePct = upside.Float64
			row.UpsideKnown = true
		}
		if rating.Valid {
			row.RatingScore = rating.Float64
			row.RatingKnown = true
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func loadForecastQualityRows(db *sql.DB, accountID string, bounds periodBounds) ([]forecastQualityRow, error) {
	rows, err := db.Query(`
		SELECT
			b.horizon_code,
			COUNT(*) AS samples,
			AVG(CASE WHEN b.target_error_abs_rub_nano IS NOT NULL THEN b.target_error_abs_rub_nano END) AS mae_rub,
			AVG(ABS(COALESCE(b.target_error_pct, 0))) AS mape_pct,
			AVG(CASE WHEN b.direction_hit = 1 THEN 100.0 ELSE 0.0 END) AS direction_hit_pct
		FROM forecast_backtest_results b
		JOIN forecast_consensus_snapshots s ON s.id = b.snapshot_id
		WHERE s.account_id = ?
			AND s.snapshot_at BETWEEN ? AND ?
		GROUP BY b.horizon_code
	`,
		accountID,
		bounds.from.UTC().Format(time.RFC3339Nano),
		bounds.to.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]forecastQualityRow, 0)
	for rows.Next() {
		var (
			row    forecastQualityRow
			maeRub sql.NullFloat64
			mape   sql.NullFloat64
			hit    sql.NullFloat64
		)
		if err := rows.Scan(&row.HorizonCode, &row.Samples, &maeRub, &mape, &hit); err != nil {
			return nil, err
		}
		if maeRub.Valid {
			row.MAERUBNano = int64(math.Round(maeRub.Float64))
		}
		if mape.Valid {
			row.MAPEPercent = mape.Float64
		}
		if hit.Valid {
			row.DirectionHitPct = hit.Float64
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.SliceStable(out, func(i, j int) bool {
		mi := forecastHorizonSortKey(out[i].HorizonCode)
		mj := forecastHorizonSortKey(out[j].HorizonCode)
		if mi != mj {
			return mi < mj
		}
		return out[i].HorizonCode < out[j].HorizonCode
	})
	return out, nil
}

func loadForecastSnapshotHistory(db *sql.DB, accountID, instrumentID string, bounds periodBounds) ([]forecastSnapshotHistoryRow, error) {
	rows, err := db.Query(`
		SELECT
			id,
			snapshot_at,
			price_at_snapshot,
			COALESCE(price_currency, ''),
			consensus_target_price,
			COALESCE(consensus_target_currency, ''),
			consensus_upside_pct,
			consensus_rating_score,
			providers_count,
			COALESCE(providers_used, '')
		FROM forecast_consensus_snapshots
		WHERE account_id = ?
			AND instrument_id = ?
			AND snapshot_at BETWEEN ? AND ?
		ORDER BY snapshot_at DESC
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

	out := make([]forecastSnapshotHistoryRow, 0)
	for rows.Next() {
		var (
			row         forecastSnapshotHistoryRow
			snapshotRaw string
			price       sql.NullFloat64
			target      sql.NullFloat64
			upside      sql.NullFloat64
			rating      sql.NullFloat64
		)
		if err := rows.Scan(
			&row.SnapshotID,
			&snapshotRaw,
			&price,
			&row.PriceCurrency,
			&target,
			&row.TargetCurrency,
			&upside,
			&rating,
			&row.ProvidersCount,
			&row.ProvidersUsed,
		); err != nil {
			return nil, err
		}
		row.SnapshotAt, err = time.Parse(time.RFC3339Nano, snapshotRaw)
		if err != nil {
			return nil, fmt.Errorf("parse snapshot_at: %w", err)
		}
		if price.Valid {
			if n, ok := floatToNano(price.Float64); ok {
				row.PriceNano = n
			}
		}
		if target.Valid {
			if n, ok := floatToNano(target.Float64); ok {
				row.TargetNano = n
			}
		}
		if upside.Valid {
			row.UpsidePct = upside.Float64
			row.UpsideKnown = true
		}
		if rating.Valid {
			row.RatingScore = rating.Float64
			row.RatingKnown = true
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func loadForecastBacktestHistory(db *sql.DB, accountID, instrumentID string, bounds periodBounds) ([]forecastBacktestHistoryRow, error) {
	rows, err := db.Query(`
		SELECT
			s.snapshot_at,
			b.horizon_code,
			b.horizon_at,
			b.actual_price,
			COALESCE(b.actual_currency, ''),
			b.target_error_pct,
			b.target_error_abs_rub_nano,
			b.direction_hit
		FROM forecast_backtest_results b
		JOIN forecast_consensus_snapshots s ON s.id = b.snapshot_id
		WHERE b.account_id = ?
			AND s.instrument_id = ?
			AND s.snapshot_at BETWEEN ? AND ?
		ORDER BY s.snapshot_at DESC, b.horizon_code
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

	out := make([]forecastBacktestHistoryRow, 0)
	for rows.Next() {
		var (
			row          forecastBacktestHistoryRow
			snapshotRaw  string
			horizonRaw   string
			actual       sql.NullFloat64
			errorPct     sql.NullFloat64
			errorRub     sql.NullInt64
			directionHit int
		)
		if err := rows.Scan(
			&snapshotRaw,
			&row.HorizonCode,
			&horizonRaw,
			&actual,
			&row.ActualCurrency,
			&errorPct,
			&errorRub,
			&directionHit,
		); err != nil {
			return nil, err
		}
		row.SnapshotAt, err = time.Parse(time.RFC3339Nano, snapshotRaw)
		if err != nil {
			return nil, fmt.Errorf("parse backtest snapshot_at: %w", err)
		}
		row.HorizonAt, err = time.Parse(time.RFC3339Nano, horizonRaw)
		if err != nil {
			return nil, fmt.Errorf("parse backtest horizon_at: %w", err)
		}
		if actual.Valid {
			if n, ok := floatToNano(actual.Float64); ok {
				row.ActualPriceNano = n
			}
		}
		if errorPct.Valid {
			row.ErrorPct = errorPct.Float64
			row.ErrorPctKnown = true
		}
		if errorRub.Valid {
			row.ErrorRUBNano = errorRub.Int64
			row.ErrorRUBKnown = true
		}
		row.DirectionHit = directionHit == 1
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func forecastHorizonSortKey(code string) int {
	code = strings.ToUpper(strings.TrimSpace(code))
	if !strings.HasSuffix(code, "M") {
		return 1 << 30
	}
	n, err := parsePositiveInt(strings.TrimSuffix(code, "M"))
	if err != nil {
		return 1 << 30
	}
	return n
}
