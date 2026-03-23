package main

import (
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"html/template"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	pb "github.com/tinkoff/invest-api-go-sdk/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type webApp struct {
	db        *sql.DB
	api       *apiClients
	cfg       config
	accountID string
}

type indexPageData struct {
	AccountID                 string
	From                      string
	To                        string
	Message                   string
	Warning                   string
	Error                     string
	Active                    []instrumentOverview
	Blocked                   []instrumentOverview
	Inactive                  []instrumentOverview
	USD                       []instrumentOverview
	EUR                       []instrumentOverview
	ActivePeriodTotalNano     int64
	ActiveLifetimeTotalNano   int64
	BlockedPeriodTotalNano    int64
	BlockedLifetimeTotalNano  int64
	InactivePeriodTotalNano   int64
	InactiveLifetimeTotalNano int64
	USDPeriodTotalNano        int64
	USDLifetimeTotalNano      int64
	EURPeriodTotalNano        int64
	EURLifetimeTotalNano      int64
}

type detailsPageData struct {
	AccountID            string
	InstrumentID         string
	Ticker               string
	Name                 string
	Currency             string
	From                 string
	To                   string
	DealsPeriodPNLNano   int64
	DealsLifetimePNLNano int64
	DividendPeriodNano   int64
	DividendLifetimeNano int64
	PeriodPNLNano        int64
	LifetimePNLNano      int64
	Rows                 []closedDealRow
	Dividends            []dividendRow
	Error                string
}

type openDealsPageData struct {
	AccountID            string
	Rows                 []openDealRow
	ByInstrument         []openDealsInstrumentSummary
	ByInstrumentBlocked  []openDealsInstrumentSummary
	ByInstrumentUSD      []openDealsInstrumentSummary
	ByInstrumentEUR      []openDealsInstrumentSummary
	LotsCount            int
	InstrumentsCount     int
	TotalOpenQty         int64
	TotalOpenCostNano    int64
	TotalBrokerPNLNano   int64
	MainLotsCount        int
	MainOpenQty          int64
	MainBrokerPNLNano    int64
	BlockedLotsCount     int
	BlockedOpenQty       int64
	BlockedBrokerPNLNano int64
	USDLotsCount         int
	USDOpenQty           int64
	USDBrokerPNLNano     int64
	EURLotsCount         int
	EUROpenQty           int64
	EURBrokerPNLNano     int64
	BrokerDataCount      int
	Warning              string
	Error                string
}

type openDealsInstrumentSummary struct {
	InstrumentID           string
	Ticker                 string
	Name                   string
	Currency               string
	LotsCount              int
	OpenQty                int64
	OpenCostNano           int64
	AvgCostNano            int64
	BrokerAvgPriceNano     int64
	BrokerCurrentPriceNano int64
	BrokerPNLNano          int64
	BrokerPNLAvailable     bool
	BrokerDataAvailable    bool
}

type moexBlueChipsPageData struct {
	AccountID       string
	From            string
	To              string
	AsOf            string
	Rows            []moexBlueChipRow
	OwnedCount      int
	PeriodTotalNano int64
	Warning         string
	Error           string
}

type moexBlueChipRow struct {
	Ticker             string
	InIndexFrom        string
	InIndexTill        string
	DividendStatus     string
	InPortfolio        bool
	InstrumentID       string
	Name               string
	Currency           string
	Status             string
	OpenQty            int64
	PeriodPNLNano      int64
	PeriodPNLAvailable bool
}

var tplFuncMap = template.FuncMap{
	"formatMoney": formatNano,
	"formatTime":  formatTimeShort,
	"pathEscape":  func(v string) string { return url.PathEscape(v) },
}

var indexTemplate = template.Must(template.New("index").Funcs(tplFuncMap).Parse(`<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>T-Invest PnL Analyzer</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-100 text-slate-900">
  <main class="max-w-7xl mx-auto p-4 md:p-8 space-y-6">
    <header class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
      <h1 class="text-2xl md:text-3xl font-semibold">T-Invest PnL Analyzer</h1>
      <p class="text-sm text-slate-600 mt-1">Account: <span class="font-mono">{{.AccountID}}</span></p>
	      <p class="text-xs text-slate-500 mt-1">PnL считается по закрытым сделкам (FIFO). На странице Instruments значения PnL и ИТОГО конвертируются в RUB по актуальному FX курсу. Инструменты с валютой торгов USD/EUR вынесены в отдельные списки.</p>
	      <div class="mt-3 text-sm">
	        <a href="/" class="text-blue-700 hover:underline mr-3">Instruments</a>
	        <a href="/open-deals" class="text-blue-700 hover:underline mr-3">Open Deals</a>
	        <a href="/moex-bluechips?from={{.From}}&to={{.To}}" class="text-blue-700 hover:underline mr-3">MOEX Blue Chips</a>
	        <a href="/forecasts?from={{.From}}&to={{.To}}" class="text-blue-700 hover:underline mr-3">Forecasts</a>
	        <a href="/forecast-quality?from={{.From}}&to={{.To}}" class="text-blue-700 hover:underline">Forecast Quality</a>
	      </div>
    </header>

	    {{if .Message}}
	    <div class="rounded-xl bg-emerald-50 text-emerald-800 border border-emerald-200 p-3 text-sm">{{.Message}}</div>
	    {{end}}
	    {{if .Warning}}
	    <div class="rounded-xl bg-amber-50 text-amber-900 border border-amber-200 p-3 text-sm">{{.Warning}}</div>
	    {{end}}
	    {{if .Error}}
	    <div class="rounded-xl bg-rose-50 text-rose-800 border border-rose-200 p-3 text-sm">{{.Error}}</div>
	    {{end}}

    <section class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
      <form method="get" action="/" class="grid grid-cols-1 md:grid-cols-5 gap-3 items-end">
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
      <form method="post" action="/sync" class="mt-3">
        <input type="hidden" name="from" value="{{.From}}" />
        <input type="hidden" name="to" value="{{.To}}" />
        <button class="h-10 rounded-lg bg-blue-600 text-white px-4">Sync & Recalculate</button>
      </form>
    </section>

    <section class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
      <h2 class="text-xl font-semibold mb-3">All Instruments (Active + Inactive)</h2>
      <div class="overflow-x-auto">
        <table class="min-w-full text-sm">
	          <thead class="text-slate-500 text-left">
	            <tr>
	              <th class="py-2 pr-4">Instrument</th>
	              <th class="py-2 pr-4">Open Qty</th>
			              <th class="py-2 pr-4">Period PnL (RUB)</th>
	              <th class="py-2 pr-4">Deals</th>
	              <th class="py-2 pr-4">Status</th>
	            </tr>
	          </thead>
          <tbody>
            {{if .Active}}
              {{range .Active}}
              <tr class="border-t border-slate-100">
                <td class="py-2 pr-4">
                  <a class="text-blue-700 hover:underline font-medium" href="/instrument/{{pathEscape .InstrumentID}}?from={{$.From}}&to={{$.To}}">{{.Ticker}}</a>
                  <div class="text-xs text-slate-500">{{.Name}} · {{.Currency}}</div>
                </td>
                <td class="py-2 pr-4 font-mono">{{.OpenQty}}</td>
		                {{if .PeriodPNLAvailable}}
		                <td class="py-2 pr-4 font-mono">{{formatMoney .PeriodPNLNano}}</td>
		                {{else}}
		                <td class="py-2 pr-4 text-slate-500">N/A</td>
		                {{end}}
	                <td class="py-2 pr-4 font-mono">{{.DealsCount}}</td>
	                <td class="py-2 pr-4 text-xs text-slate-600">{{.Status}}</td>
	              </tr>
	              {{end}}
		              <tr class="border-t-2 border-slate-300 bg-slate-50 font-semibold">
		                <td class="py-2 pr-4">ИТОГО (RUB)</td>
	                <td class="py-2 pr-4"></td>
	                <td class="py-2 pr-4 font-mono">{{formatMoney .ActivePeriodTotalNano}}</td>
	                <td class="py-2 pr-4"></td>
	                <td class="py-2 pr-4"></td>
	              </tr>
	            {{else}}
	              <tr><td colspan="5" class="py-3 text-slate-500">No instruments.</td></tr>
	            {{end}}
	          </tbody>
	        </table>
	      </div>
		    </section>

	    <section class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
	      <h2 class="text-xl font-semibold mb-3">Blocked Instruments</h2>
	      <div class="overflow-x-auto">
	        <table class="min-w-full text-sm">
		          <thead class="text-slate-500 text-left">
		            <tr>
		              <th class="py-2 pr-4">Instrument</th>
		              <th class="py-2 pr-4">Open Qty</th>
		              <th class="py-2 pr-4">Period PnL (RUB)</th>
		              <th class="py-2 pr-4">Deals</th>
		            </tr>
		          </thead>
	          <tbody>
	            {{if .Blocked}}
	              {{range .Blocked}}
	              <tr class="border-t border-slate-100">
	                <td class="py-2 pr-4">
	                  <a class="text-blue-700 hover:underline font-medium" href="/instrument/{{pathEscape .InstrumentID}}?from={{$.From}}&to={{$.To}}">{{.Ticker}}</a>
	                  <div class="text-xs text-slate-500">{{.Name}} · {{.Currency}}</div>
	                </td>
	                <td class="py-2 pr-4 font-mono">{{.OpenQty}}</td>
		                {{if .PeriodPNLAvailable}}
		                <td class="py-2 pr-4 font-mono">{{formatMoney .PeriodPNLNano}}</td>
		                {{else}}
		                <td class="py-2 pr-4 text-slate-500">N/A</td>
		                {{end}}
		                <td class="py-2 pr-4 font-mono">{{.DealsCount}}</td>
		              </tr>
		              {{end}}
	              <tr class="border-t-2 border-slate-300 bg-slate-50 font-semibold">
		                <td class="py-2 pr-4">ИТОГО (RUB)</td>
		                <td class="py-2 pr-4"></td>
		                <td class="py-2 pr-4 font-mono">{{formatMoney .BlockedPeriodTotalNano}}</td>
		                <td class="py-2 pr-4"></td>
		              </tr>
		            {{else}}
		              <tr><td colspan="4" class="py-3 text-slate-500">No blocked instruments.</td></tr>
		            {{end}}
		          </tbody>
		        </table>
		      </div>
	    </section>

		    <section class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
		      <h2 class="text-xl font-semibold mb-3">USD Instruments</h2>
	      <div class="overflow-x-auto">
	        <table class="min-w-full text-sm">
		          <thead class="text-slate-500 text-left">
		            <tr>
		              <th class="py-2 pr-4">Instrument</th>
		              <th class="py-2 pr-4">Open Qty</th>
		              <th class="py-2 pr-4">Period PnL (RUB)</th>
		              <th class="py-2 pr-4">Deals</th>
		            </tr>
		          </thead>
	          <tbody>
	            {{if .USD}}
	              {{range .USD}}
	              <tr class="border-t border-slate-100">
	                <td class="py-2 pr-4">
	                  <a class="text-blue-700 hover:underline font-medium" href="/instrument/{{pathEscape .InstrumentID}}?from={{$.From}}&to={{$.To}}">{{.Ticker}}</a>
	                  <div class="text-xs text-slate-500">{{.Name}} · {{.Currency}}</div>
	                </td>
	                <td class="py-2 pr-4 font-mono">{{.OpenQty}}</td>
		                {{if .PeriodPNLAvailable}}
		                <td class="py-2 pr-4 font-mono">{{formatMoney .PeriodPNLNano}}</td>
		                {{else}}
		                <td class="py-2 pr-4 text-slate-500">N/A</td>
		                {{end}}
		                <td class="py-2 pr-4 font-mono">{{.DealsCount}}</td>
		              </tr>
		              {{end}}
	              <tr class="border-t-2 border-slate-300 bg-slate-50 font-semibold">
		                <td class="py-2 pr-4">ИТОГО (RUB)</td>
		                <td class="py-2 pr-4"></td>
		                <td class="py-2 pr-4 font-mono">{{formatMoney .USDPeriodTotalNano}}</td>
		                <td class="py-2 pr-4"></td>
		              </tr>
		            {{else}}
		              <tr><td colspan="4" class="py-3 text-slate-500">No USD instruments.</td></tr>
		            {{end}}
		          </tbody>
		        </table>
		      </div>
	    </section>

	    <section class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
	      <h2 class="text-xl font-semibold mb-3">EUR Instruments</h2>
	      <div class="overflow-x-auto">
	        <table class="min-w-full text-sm">
		          <thead class="text-slate-500 text-left">
		            <tr>
		              <th class="py-2 pr-4">Instrument</th>
		              <th class="py-2 pr-4">Open Qty</th>
		              <th class="py-2 pr-4">Period PnL (RUB)</th>
		              <th class="py-2 pr-4">Deals</th>
		            </tr>
		          </thead>
	          <tbody>
	            {{if .EUR}}
	              {{range .EUR}}
	              <tr class="border-t border-slate-100">
	                <td class="py-2 pr-4">
	                  <a class="text-blue-700 hover:underline font-medium" href="/instrument/{{pathEscape .InstrumentID}}?from={{$.From}}&to={{$.To}}">{{.Ticker}}</a>
	                  <div class="text-xs text-slate-500">{{.Name}} · {{.Currency}}</div>
	                </td>
	                <td class="py-2 pr-4 font-mono">{{.OpenQty}}</td>
		                {{if .PeriodPNLAvailable}}
		                <td class="py-2 pr-4 font-mono">{{formatMoney .PeriodPNLNano}}</td>
		                {{else}}
		                <td class="py-2 pr-4 text-slate-500">N/A</td>
		                {{end}}
		                <td class="py-2 pr-4 font-mono">{{.DealsCount}}</td>
		              </tr>
		              {{end}}
	              <tr class="border-t-2 border-slate-300 bg-slate-50 font-semibold">
		                <td class="py-2 pr-4">ИТОГО (RUB)</td>
		                <td class="py-2 pr-4"></td>
		                <td class="py-2 pr-4 font-mono">{{formatMoney .EURPeriodTotalNano}}</td>
		                <td class="py-2 pr-4"></td>
		              </tr>
		            {{else}}
		              <tr><td colspan="4" class="py-3 text-slate-500">No EUR instruments.</td></tr>
		            {{end}}
		          </tbody>
		        </table>
	      </div>
		    </section>

	  </main>
</body>
</html>`))

var detailsTemplate = template.Must(template.New("details").Funcs(tplFuncMap).Parse(`<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Instrument Deals</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-100 text-slate-900">
  <main class="max-w-7xl mx-auto p-4 md:p-8 space-y-6">
    <header class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
	      <a href="/?from={{.From}}&to={{.To}}" class="text-sm text-blue-700 hover:underline">← Back to instruments</a>
	      <a href="/open-deals" class="ml-4 text-sm text-blue-700 hover:underline">Open Deals</a>
	      <a href="/moex-bluechips?from={{.From}}&to={{.To}}" class="ml-4 text-sm text-blue-700 hover:underline">MOEX Blue Chips</a>
	      <a href="/forecasts?from={{.From}}&to={{.To}}" class="ml-4 text-sm text-blue-700 hover:underline">Forecasts</a>
	      <a href="/forecast-quality?from={{.From}}&to={{.To}}" class="ml-4 text-sm text-blue-700 hover:underline">Forecast Quality</a>
      <h1 class="text-2xl font-semibold mt-2">{{.Ticker}}</h1>
      <p class="text-sm text-slate-600">{{.Name}} · {{.Currency}}</p>
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
      <p class="mt-3 text-sm text-slate-700">Deals Period PnL: <span class="font-mono font-semibold">{{formatMoney .DealsPeriodPNLNano}}</span></p>
      <p class="mt-1 text-sm text-slate-700">Dividends Period: <span class="font-mono font-semibold">{{formatMoney .DividendPeriodNano}}</span></p>
      <p class="mt-1 text-sm text-slate-900 font-semibold">Period PnL (Deals + Dividends): <span class="font-mono">{{formatMoney .PeriodPNLNano}}</span></p>
      <p class="mt-2 text-sm text-slate-700">Deals Lifetime PnL: <span class="font-mono font-semibold">{{formatMoney .DealsLifetimePNLNano}}</span></p>
      <p class="mt-1 text-sm text-slate-700">Dividends Lifetime: <span class="font-mono font-semibold">{{formatMoney .DividendLifetimeNano}}</span></p>
      <p class="mt-1 text-sm text-slate-900 font-semibold">Lifetime PnL (Deals + Dividends): <span class="font-mono">{{formatMoney .LifetimePNLNano}}</span></p>
    </section>

    <section class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
      <h2 class="text-xl font-semibold mb-3">Closed Deals</h2>
      <div class="overflow-x-auto">
        <table class="min-w-full text-sm">
          <thead class="text-slate-500 text-left">
            <tr>
              <th class="py-2 pr-4">Close Time</th>
              <th class="py-2 pr-4">Buy Sum (Qty)</th>
              <th class="py-2 pr-4">Sell Sum (Qty)</th>
              <th class="py-2 pr-4">Final PnL</th>
              <th class="py-2 pr-4">Buy Op</th>
              <th class="py-2 pr-4">Sell Op</th>
            </tr>
          </thead>
          <tbody>
            {{if .Rows}}
              {{range .Rows}}
              <tr class="border-t border-slate-100">
                <td class="py-2 pr-4">{{formatTime .CloseTime}}</td>
                <td class="py-2 pr-4 font-mono">{{formatMoney .BuySumNano}} ({{.BuyQty}})</td>
                <td class="py-2 pr-4 font-mono">{{formatMoney .SellSumNano}} ({{.SellQty}})</td>
                <td class="py-2 pr-4 font-mono">{{formatMoney .FinalPNLNano}}</td>
                <td class="py-2 pr-4 font-mono text-xs">{{.BuyOperation}}</td>
                <td class="py-2 pr-4 font-mono text-xs">{{.SellOperation}}</td>
              </tr>
              {{end}}
              <tr class="border-t-2 border-slate-300 bg-slate-50 font-semibold">
                <td class="py-2 pr-4" colspan="3">ИТОГО</td>
                <td class="py-2 pr-4 font-mono">
                  Period (Deals + Dividends): {{formatMoney $.PeriodPNLNano}}<br/>
                  Lifetime (Deals + Dividends): {{formatMoney $.LifetimePNLNano}}
                </td>
                <td class="py-2 pr-4" colspan="2"></td>
              </tr>
            {{else}}
              <tr><td colspan="6" class="py-3 text-slate-500">No closed deals for selected period.</td></tr>
            {{end}}
          </tbody>
        </table>
      </div>
    </section>

    <section class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
      <h2 class="text-xl font-semibold mb-3">Dividends</h2>
      <div class="overflow-x-auto">
        <table class="min-w-full text-sm">
          <thead class="text-slate-500 text-left">
            <tr>
              <th class="py-2 pr-4">Date</th>
              <th class="py-2 pr-4">Operation Type</th>
              <th class="py-2 pr-4">Amount</th>
              <th class="py-2 pr-4">Operation ID</th>
            </tr>
          </thead>
          <tbody>
            {{if .Dividends}}
              {{range .Dividends}}
              <tr class="border-t border-slate-100">
                <td class="py-2 pr-4">{{formatTime .ExecutedAt}}</td>
                <td class="py-2 pr-4 font-mono text-xs">{{.OperationType}}</td>
                <td class="py-2 pr-4 font-mono">{{formatMoney .AmountNano}}</td>
                <td class="py-2 pr-4 font-mono text-xs">{{.OperationID}}</td>
              </tr>
              {{end}}
              <tr class="border-t-2 border-slate-300 bg-slate-50 font-semibold">
                <td class="py-2 pr-4" colspan="2">ИТОГО ДИВИДЕНДЫ</td>
                <td class="py-2 pr-4 font-mono">Period: {{formatMoney .DividendPeriodNano}}<br/>Lifetime: {{formatMoney .DividendLifetimeNano}}</td>
                <td class="py-2 pr-4"></td>
              </tr>
            {{else}}
              <tr><td colspan="4" class="py-3 text-slate-500">No dividends for selected period.</td></tr>
            {{end}}
          </tbody>
        </table>
      </div>
    </section>
  </main>
</body>
</html>`))

var openDealsTemplate = template.Must(template.New("openDeals").Funcs(tplFuncMap).Parse(`<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Open Deals</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-100 text-slate-900">
	  <main class="max-w-7xl mx-auto p-4 md:p-8 space-y-6">
	    <header class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
	      <h1 class="text-2xl md:text-3xl font-semibold">Open Deals</h1>
	      <p class="text-sm text-slate-600 mt-1">Account: <span class="font-mono">{{.AccountID}}</span></p>
	      <div class="mt-3 text-sm">
	        <a href="/" class="text-blue-700 hover:underline mr-3">Instruments</a>
	        <a href="/open-deals" class="text-blue-700 hover:underline mr-3">Open Deals</a>
	        <a href="/moex-bluechips" class="text-blue-700 hover:underline mr-3">MOEX Blue Chips</a>
	        <a href="/forecasts" class="text-blue-700 hover:underline mr-3">Forecasts</a>
	        <a href="/forecast-quality" class="text-blue-700 hover:underline">Forecast Quality</a>
	      </div>
	      <p class="mt-3 text-sm text-slate-700">Instruments: <span class="font-mono font-semibold">{{.InstrumentsCount}}</span></p>
	      <p class="mt-1 text-sm text-slate-700">Total open qty: <span class="font-mono font-semibold">{{.TotalOpenQty}}</span></p>
	    </header>

	    {{if .Error}}
	    <div class="rounded-xl bg-rose-50 text-rose-800 border border-rose-200 p-3 text-sm">{{.Error}}</div>
	    {{end}}
	    {{if .Warning}}
	    <div class="rounded-xl bg-amber-50 text-amber-900 border border-amber-200 p-3 text-sm">{{.Warning}}</div>
	    {{end}}

	    <section class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
	      <h2 class="text-xl font-semibold mb-3">Open By Instrument (RUB/Other)</h2>
      <div class="overflow-x-auto">
        <table class="min-w-full text-sm">
	          <thead class="text-slate-500 text-left">
			            <tr>
			              <th class="py-2 pr-4">Instrument</th>
			              <th class="py-2 pr-4">Open Buys</th>
			              <th class="py-2 pr-4">Open Qty</th>
			              <th class="py-2 pr-4">Avg Cost (Broker)</th>
			              <th class="py-2 pr-4">Current Price (Broker)</th>
				              <th class="py-2 pr-4">Current PnL (Broker, RUB)</th>
			            </tr>
		          </thead>
          <tbody>
            {{if .ByInstrument}}
              {{range .ByInstrument}}
              <tr class="border-t border-slate-100">
                <td class="py-2 pr-4">
                  <span class="font-medium">{{.Ticker}}</span>
                  <div class="text-xs text-slate-500">{{.Name}} · {{.Currency}}</div>
	                </td>
	                <td class="py-2 pr-4 font-mono">{{.LotsCount}}</td>
			                <td class="py-2 pr-4 font-mono">{{.OpenQty}}</td>
				                {{if .BrokerDataAvailable}}
				                {{if ne .BrokerAvgPriceNano 0}}
				                <td class="py-2 pr-4 font-mono">{{formatMoney .BrokerAvgPriceNano}}</td>
				                {{else}}
				                <td class="py-2 pr-4 text-slate-500">N/A</td>
				                {{end}}
				                {{if ne .BrokerCurrentPriceNano 0}}
				                <td class="py-2 pr-4 font-mono">{{formatMoney .BrokerCurrentPriceNano}}</td>
				                {{else}}
				                <td class="py-2 pr-4 text-slate-500">N/A</td>
				                {{end}}
				                {{if .BrokerPNLAvailable}}
				                <td class="py-2 pr-4 font-mono">{{formatMoney .BrokerPNLNano}}</td>
				                {{else}}
				                <td class="py-2 pr-4 text-slate-500">N/A</td>
				                {{end}}
				                {{else}}
				                <td class="py-2 pr-4 text-slate-500">N/A</td>
				                <td class="py-2 pr-4 text-slate-500">N/A</td>
			                <td class="py-2 pr-4 text-slate-500">N/A</td>
			                {{end}}
		              </tr>
	              {{end}}
		              <tr class="border-t-2 border-slate-300 bg-slate-50 font-semibold">
		                <td class="py-2 pr-4">ИТОГО (RUB)</td>
	                <td class="py-2 pr-4 font-mono">{{.MainLotsCount}}</td>
		                <td class="py-2 pr-4 font-mono">{{.MainOpenQty}}</td>
		                <td class="py-2 pr-4"></td>
		                <td class="py-2 pr-4"></td>
		                <td class="py-2 pr-4 font-mono">{{formatMoney .MainBrokerPNLNano}}</td>
	              </tr>
			              <tr class="bg-slate-50">
			                <td colspan="6" class="py-2 pr-4 text-xs text-slate-600">
					                  Broker PnL получен для {{.BrokerDataCount}} из {{.InstrumentsCount}} инструментов. Колонка Current PnL и ИТОГО пересчитаны в RUB по FX-курсу. Списки отсортированы по Current PnL (RUB) по убыванию.
					                </td>
			              </tr>
				            {{else}}
				              <tr><td colspan="6" class="py-3 text-slate-500">No open RUB/other instruments.</td></tr>
				            {{end}}
		          </tbody>
		        </table>
	      </div>
	    </section>

	    <section class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
	      <h2 class="text-xl font-semibold mb-3">Open By Instrument (USD)</h2>
	      <div class="overflow-x-auto">
	        <table class="min-w-full text-sm">
	          <thead class="text-slate-500 text-left">
	            <tr>
	              <th class="py-2 pr-4">Instrument</th>
	              <th class="py-2 pr-4">Open Buys</th>
	              <th class="py-2 pr-4">Open Qty</th>
	              <th class="py-2 pr-4">Avg Cost (Broker)</th>
	              <th class="py-2 pr-4">Current Price (Broker)</th>
	              <th class="py-2 pr-4">Current PnL (Broker, RUB)</th>
	            </tr>
	          </thead>
	          <tbody>
	            {{if .ByInstrumentUSD}}
	              {{range .ByInstrumentUSD}}
	              <tr class="border-t border-slate-100">
	                <td class="py-2 pr-4">
	                  <span class="font-medium">{{.Ticker}}</span>
	                  <div class="text-xs text-slate-500">{{.Name}} · {{.Currency}}</div>
	                </td>
	                <td class="py-2 pr-4 font-mono">{{.LotsCount}}</td>
	                <td class="py-2 pr-4 font-mono">{{.OpenQty}}</td>
	                {{if .BrokerDataAvailable}}
	                {{if ne .BrokerAvgPriceNano 0}}
	                <td class="py-2 pr-4 font-mono">{{formatMoney .BrokerAvgPriceNano}}</td>
	                {{else}}
	                <td class="py-2 pr-4 text-slate-500">N/A</td>
	                {{end}}
	                {{if ne .BrokerCurrentPriceNano 0}}
	                <td class="py-2 pr-4 font-mono">{{formatMoney .BrokerCurrentPriceNano}}</td>
	                {{else}}
	                <td class="py-2 pr-4 text-slate-500">N/A</td>
	                {{end}}
	                {{if .BrokerPNLAvailable}}
	                <td class="py-2 pr-4 font-mono">{{formatMoney .BrokerPNLNano}}</td>
	                {{else}}
	                <td class="py-2 pr-4 text-slate-500">N/A</td>
	                {{end}}
	                {{else}}
	                <td class="py-2 pr-4 text-slate-500">N/A</td>
	                <td class="py-2 pr-4 text-slate-500">N/A</td>
	                <td class="py-2 pr-4 text-slate-500">N/A</td>
	                {{end}}
	              </tr>
	              {{end}}
	              <tr class="border-t-2 border-slate-300 bg-slate-50 font-semibold">
	                <td class="py-2 pr-4">ИТОГО (RUB)</td>
	                <td class="py-2 pr-4 font-mono">{{.USDLotsCount}}</td>
	                <td class="py-2 pr-4 font-mono">{{.USDOpenQty}}</td>
	                <td class="py-2 pr-4"></td>
	                <td class="py-2 pr-4"></td>
	                <td class="py-2 pr-4 font-mono">{{formatMoney .USDBrokerPNLNano}}</td>
	              </tr>
	            {{else}}
	              <tr><td colspan="6" class="py-3 text-slate-500">No open USD instruments.</td></tr>
	            {{end}}
	          </tbody>
	        </table>
	      </div>
	    </section>

	    <section class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
	      <h2 class="text-xl font-semibold mb-3">Open By Instrument (EUR)</h2>
	      <div class="overflow-x-auto">
	        <table class="min-w-full text-sm">
	          <thead class="text-slate-500 text-left">
	            <tr>
	              <th class="py-2 pr-4">Instrument</th>
	              <th class="py-2 pr-4">Open Buys</th>
	              <th class="py-2 pr-4">Open Qty</th>
	              <th class="py-2 pr-4">Avg Cost (Broker)</th>
	              <th class="py-2 pr-4">Current Price (Broker)</th>
	              <th class="py-2 pr-4">Current PnL (Broker, RUB)</th>
	            </tr>
	          </thead>
	          <tbody>
	            {{if .ByInstrumentEUR}}
	              {{range .ByInstrumentEUR}}
	              <tr class="border-t border-slate-100">
	                <td class="py-2 pr-4">
	                  <span class="font-medium">{{.Ticker}}</span>
	                  <div class="text-xs text-slate-500">{{.Name}} · {{.Currency}}</div>
	                </td>
	                <td class="py-2 pr-4 font-mono">{{.LotsCount}}</td>
	                <td class="py-2 pr-4 font-mono">{{.OpenQty}}</td>
	                {{if .BrokerDataAvailable}}
	                {{if ne .BrokerAvgPriceNano 0}}
	                <td class="py-2 pr-4 font-mono">{{formatMoney .BrokerAvgPriceNano}}</td>
	                {{else}}
	                <td class="py-2 pr-4 text-slate-500">N/A</td>
	                {{end}}
	                {{if ne .BrokerCurrentPriceNano 0}}
	                <td class="py-2 pr-4 font-mono">{{formatMoney .BrokerCurrentPriceNano}}</td>
	                {{else}}
	                <td class="py-2 pr-4 text-slate-500">N/A</td>
	                {{end}}
	                {{if .BrokerPNLAvailable}}
	                <td class="py-2 pr-4 font-mono">{{formatMoney .BrokerPNLNano}}</td>
	                {{else}}
	                <td class="py-2 pr-4 text-slate-500">N/A</td>
	                {{end}}
	                {{else}}
	                <td class="py-2 pr-4 text-slate-500">N/A</td>
	                <td class="py-2 pr-4 text-slate-500">N/A</td>
	                <td class="py-2 pr-4 text-slate-500">N/A</td>
	                {{end}}
	              </tr>
	              {{end}}
	              <tr class="border-t-2 border-slate-300 bg-slate-50 font-semibold">
	                <td class="py-2 pr-4">ИТОГО (RUB)</td>
	                <td class="py-2 pr-4 font-mono">{{.EURLotsCount}}</td>
	                <td class="py-2 pr-4 font-mono">{{.EUROpenQty}}</td>
	                <td class="py-2 pr-4"></td>
	                <td class="py-2 pr-4"></td>
	                <td class="py-2 pr-4 font-mono">{{formatMoney .EURBrokerPNLNano}}</td>
	              </tr>
	            {{else}}
	              <tr><td colspan="6" class="py-3 text-slate-500">No open EUR instruments.</td></tr>
	            {{end}}
	          </tbody>
	        </table>
	      </div>
	    </section>

	    <section class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
	      <h2 class="text-xl font-semibold mb-3">Open By Instrument (Blocked)</h2>
      <div class="overflow-x-auto">
        <table class="min-w-full text-sm">
	          <thead class="text-slate-500 text-left">
	            <tr>
	              <th class="py-2 pr-4">Instrument</th>
	              <th class="py-2 pr-4">Open Buys</th>
	              <th class="py-2 pr-4">Open Qty</th>
	              <th class="py-2 pr-4">Avg Cost (Broker)</th>
	              <th class="py-2 pr-4">Current Price (Broker)</th>
	              <th class="py-2 pr-4">Current PnL (Broker, RUB)</th>
	            </tr>
	          </thead>
          <tbody>
            {{if .ByInstrumentBlocked}}
              {{range .ByInstrumentBlocked}}
              <tr class="border-t border-slate-100">
                <td class="py-2 pr-4">
                  <span class="font-medium">{{.Ticker}}</span>
                  <div class="text-xs text-slate-500">{{.Name}} · {{.Currency}}</div>
                </td>
                <td class="py-2 pr-4 font-mono">{{.LotsCount}}</td>
                <td class="py-2 pr-4 font-mono">{{.OpenQty}}</td>
                {{if .BrokerDataAvailable}}
                {{if ne .BrokerAvgPriceNano 0}}
                <td class="py-2 pr-4 font-mono">{{formatMoney .BrokerAvgPriceNano}}</td>
                {{else}}
                <td class="py-2 pr-4 text-slate-500">N/A</td>
                {{end}}
                {{if ne .BrokerCurrentPriceNano 0}}
                <td class="py-2 pr-4 font-mono">{{formatMoney .BrokerCurrentPriceNano}}</td>
                {{else}}
                <td class="py-2 pr-4 text-slate-500">N/A</td>
                {{end}}
                {{if .BrokerPNLAvailable}}
                <td class="py-2 pr-4 font-mono">{{formatMoney .BrokerPNLNano}}</td>
                {{else}}
                <td class="py-2 pr-4 text-slate-500">N/A</td>
                {{end}}
                {{else}}
                <td class="py-2 pr-4 text-slate-500">N/A</td>
                <td class="py-2 pr-4 text-slate-500">N/A</td>
                <td class="py-2 pr-4 text-slate-500">N/A</td>
                {{end}}
              </tr>
              {{end}}
              <tr class="border-t-2 border-slate-300 bg-slate-50 font-semibold">
                <td class="py-2 pr-4">ИТОГО (RUB)</td>
                <td class="py-2 pr-4 font-mono">{{.BlockedLotsCount}}</td>
                <td class="py-2 pr-4 font-mono">{{.BlockedOpenQty}}</td>
                <td class="py-2 pr-4"></td>
                <td class="py-2 pr-4"></td>
                <td class="py-2 pr-4 font-mono">{{formatMoney .BlockedBrokerPNLNano}}</td>
              </tr>
            {{else}}
              <tr><td colspan="6" class="py-3 text-slate-500">No blocked open instruments.</td></tr>
            {{end}}
          </tbody>
        </table>
      </div>
	    </section>
	  </main>
</body>
</html>`))

var moexBlueChipsTemplate = template.Must(template.New("moexBlueChips").Funcs(tplFuncMap).Parse(`<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>MOEX Blue Chips</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-100 text-slate-900">
  <main class="max-w-7xl mx-auto p-4 md:p-8 space-y-6">
    <header class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
      <h1 class="text-2xl md:text-3xl font-semibold">MOEX Blue Chips (MOEXBC)</h1>
      <p class="text-sm text-slate-600 mt-1">Account: <span class="font-mono">{{.AccountID}}</span></p>
      <p class="text-xs text-slate-500 mt-1">ISS source: /iss/statistics/engines/stock/markets/index/analytics/MOEXBC/tickers</p>
      <div class="mt-3 text-sm">
        <a href="/?from={{.From}}&to={{.To}}" class="text-blue-700 hover:underline mr-3">Instruments</a>
        <a href="/open-deals" class="text-blue-700 hover:underline mr-3">Open Deals</a>
        <a href="/moex-bluechips?from={{.From}}&to={{.To}}" class="text-blue-700 hover:underline mr-3">MOEX Blue Chips</a>
        <a href="/forecasts?from={{.From}}&to={{.To}}" class="text-blue-700 hover:underline mr-3">Forecasts</a>
        <a href="/forecast-quality?from={{.From}}&to={{.To}}" class="text-blue-700 hover:underline">Forecast Quality</a>
      </div>
      <p class="mt-3 text-sm text-slate-700">As of: <span class="font-mono font-semibold">{{.AsOf}}</span></p>
      <p class="mt-1 text-sm text-slate-700">Owned in index: <span class="font-mono font-semibold">{{.OwnedCount}}</span></p>
      <p class="mt-1 text-sm text-slate-700">Owned Period PnL total (RUB): <span class="font-mono font-semibold">{{formatMoney .PeriodTotalNano}}</span></p>
    </header>

    {{if .Warning}}
    <div class="rounded-xl bg-amber-50 text-amber-900 border border-amber-200 p-3 text-sm">{{.Warning}}</div>
    {{end}}
    {{if .Error}}
    <div class="rounded-xl bg-rose-50 text-rose-800 border border-rose-200 p-3 text-sm">{{.Error}}</div>
    {{end}}

    <section class="rounded-2xl bg-white shadow-sm border border-slate-200 p-5">
      <form method="get" action="/moex-bluechips" class="grid grid-cols-1 md:grid-cols-5 gap-3 items-end">
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
      <h2 class="text-xl font-semibold mb-3">Current MOEXBC Members</h2>
      <div class="overflow-x-auto">
        <table class="min-w-full text-sm">
          <thead class="text-slate-500 text-left">
            <tr>
              <th class="py-2 pr-4">Ticker</th>
              <th class="py-2 pr-4">In Index</th>
              <th class="py-2 pr-4">Owned</th>
              <th class="py-2 pr-4">Open Qty</th>
              <th class="py-2 pr-4">Period PnL (RUB)</th>
              <th class="py-2 pr-4">Дивиденды</th>
              <th class="py-2 pr-4">Instrument</th>
            </tr>
          </thead>
          <tbody>
            {{if .Rows}}
              {{range .Rows}}
              <tr class="border-t border-slate-100">
                <td class="py-2 pr-4 font-mono">{{.Ticker}}</td>
                <td class="py-2 pr-4 text-xs text-slate-600">{{.InIndexFrom}} .. {{.InIndexTill}}</td>
                <td class="py-2 pr-4">{{if .InPortfolio}}Yes{{else}}No{{end}}</td>
                {{if .InPortfolio}}
                <td class="py-2 pr-4 font-mono">{{.OpenQty}}</td>
                {{if .PeriodPNLAvailable}}
                <td class="py-2 pr-4 font-mono">{{formatMoney .PeriodPNLNano}}</td>
                {{else}}
                <td class="py-2 pr-4 text-slate-500">N/A</td>
                {{end}}
                <td class="py-2 pr-4">{{.DividendStatus}}</td>
                <td class="py-2 pr-4">
                  <a class="text-blue-700 hover:underline font-medium" href="/instrument/{{pathEscape .InstrumentID}}?from={{$.From}}&to={{$.To}}">{{.Name}}</a>
                  <div class="text-xs text-slate-500">{{.Status}} · {{.Currency}}</div>
                </td>
                {{else}}
                <td class="py-2 pr-4 text-slate-500">-</td>
                <td class="py-2 pr-4 text-slate-500">-</td>
                <td class="py-2 pr-4">{{.DividendStatus}}</td>
                <td class="py-2 pr-4 text-slate-500">Not in portfolio</td>
                {{end}}
              </tr>
              {{end}}
            {{else}}
              <tr><td colspan="7" class="py-3 text-slate-500">No MOEXBC data.</td></tr>
            {{end}}
          </tbody>
        </table>
      </div>
    </section>
  </main>
</body>
</html>`))

func runWebServer(db *sql.DB, api *apiClients, cfg config, accountID string) error {
	app := &webApp{db: db, api: api, cfg: cfg, accountID: accountID}

	mux := http.NewServeMux()
	mux.HandleFunc("/", app.handleIndex)
	mux.HandleFunc("/sync", app.handleSync)
	mux.HandleFunc("/open-deals", app.handleOpenDeals)
	mux.HandleFunc("/moex-bluechips", app.handleMOEXBlueChips)
	mux.HandleFunc("/forecasts", app.handleForecasts)
	mux.HandleFunc("/forecast-quality", app.handleForecastQuality)
	mux.HandleFunc("/forecast/", app.handleForecastInstrument)
	mux.HandleFunc("/instrument/", app.handleInstrument)

	fmt.Printf("Web UI started: http://localhost%s\n", cfg.httpAddr)
	return http.ListenAndServe(cfg.httpAddr, mux)
}

func (a *webApp) handleSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	from := strings.TrimSpace(r.FormValue("from"))
	to := strings.TrimSpace(r.FormValue("to"))

	err := syncAndRebuild(a.db, a.api, a.cfg, a.accountID)
	msg := "Sync completed"
	if err != nil {
		msg = "Sync failed: " + err.Error()
	}

	target := "/?from=" + url.QueryEscape(from) + "&to=" + url.QueryEscape(to) + "&msg=" + url.QueryEscape(msg)
	http.Redirect(w, r, target, http.StatusSeeOther)
}

func (a *webApp) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	fromRaw := strings.TrimSpace(r.URL.Query().Get("from"))
	toRaw := strings.TrimSpace(r.URL.Query().Get("to"))
	bounds, from, to, parseErr := parseWebBounds(fromRaw, toRaw)
	periodFilterEnabled := fromRaw != "" || toRaw != ""

	data := indexPageData{
		AccountID: a.accountID,
		From:      from,
		To:        to,
		Message:   strings.TrimSpace(r.URL.Query().Get("msg")),
	}
	if parseErr != nil {
		data.Error = parseErr.Error()
		_ = indexTemplate.Execute(w, data)
		return
	}

	rows, err := loadInstrumentOverview(a.db, a.accountID, bounds)
	if err != nil {
		data.Error = err.Error()
		_ = indexTemplate.Execute(w, data)
		return
	}

	fxRates, fxWarnings := a.loadRubFXRates(collectOverviewCurrencies(rows))
	warningParts := make([]string, 0, len(fxWarnings))
	warningParts = append(warningParts, fxWarnings...)
	missingCurrencyWarnings := map[string]bool{}

	for _, row := range rows {
		if periodFilterEnabled && row.PeriodEventsCount == 0 {
			continue
		}
		periodRub, okPeriod := convertNanoToRUB(row.PeriodPNLNano, row.Currency, fxRates)
		row.PeriodPNLAvailable = okPeriod
		if okPeriod {
			row.PeriodPNLNano = periodRub
		}
		if !okPeriod {
			code := normalizeCurrency(row.Currency)
			if code == "" {
				code = "UNKNOWN"
			}
			if !missingCurrencyWarnings[code] {
				warningParts = append(warningParts, fmt.Sprintf("Не удалось конвертировать %s -> RUB для части инструментов: суммы исключены из ИТОГО.", code))
				missingCurrencyWarnings[code] = true
			}
		}

		if isBlockedInstrument(row) {
			data.Blocked = append(data.Blocked, row)
			if okPeriod {
				data.BlockedPeriodTotalNano += periodRub
			}
			continue
		}

		switch historicalCurrencyBucket(row) {
		case "USD":
			data.USD = append(data.USD, row)
			if okPeriod {
				data.USDPeriodTotalNano += periodRub
			}
			continue
		case "EUR":
			data.EUR = append(data.EUR, row)
			if okPeriod {
				data.EURPeriodTotalNano += periodRub
			}
			continue
		}

		data.Active = append(data.Active, row)
		if okPeriod {
			data.ActivePeriodTotalNano += periodRub
		}
	}
	sortOverviewByPNL(data.Active)
	sortOverviewByPNL(data.Blocked)
	sortOverviewByPNL(data.USD)
	sortOverviewByPNL(data.EUR)
	if len(warningParts) > 0 {
		data.Warning = strings.Join(warningParts, " ")
	}

	_ = indexTemplate.Execute(w, data)
}

func (a *webApp) handleMOEXBlueChips(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	fromRaw := strings.TrimSpace(r.URL.Query().Get("from"))
	toRaw := strings.TrimSpace(r.URL.Query().Get("to"))
	bounds, from, to, parseErr := parseWebBounds(fromRaw, toRaw)

	data := moexBlueChipsPageData{
		AccountID: a.accountID,
		From:      from,
		To:        to,
		AsOf:      time.Now().UTC().Format("2006-01-02"),
	}
	if parseErr != nil {
		data.Error = parseErr.Error()
		_ = moexBlueChipsTemplate.Execute(w, data)
		return
	}

	members, asOf, err := a.fetchMOEXBCActiveMembers(time.Now().UTC())
	if err != nil {
		data.Error = "Не удалось загрузить состав MOEXBC: " + err.Error()
		_ = moexBlueChipsTemplate.Execute(w, data)
		return
	}
	data.AsOf = asOf.Format("2006-01-02")
	if !asOf.IsZero() {
		today := time.Now().UTC()
		today = time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, time.UTC)
		if !asOf.Equal(today) {
			data.Warning = "Состав индекса на выбранную дату недоступен в ISS, показана последняя доступная дата."
		}
	}

	overviewRows, err := loadInstrumentOverview(a.db, a.accountID, bounds)
	if err != nil {
		data.Error = err.Error()
		_ = moexBlueChipsTemplate.Execute(w, data)
		return
	}

	byTicker := make(map[string]instrumentOverview, len(overviewRows))
	for _, row := range overviewRows {
		t := strings.ToUpper(strings.TrimSpace(row.Ticker))
		if t == "" {
			continue
		}
		if _, exists := byTicker[t]; !exists {
			byTicker[t] = row
		}
	}

	dividendInfo, dividendWarnings := a.loadBlueChipDividendInfo(members)
	fxRates, fxWarnings := a.loadRubFXRates(collectOverviewCurrencies(overviewRows))
	warningParts := make([]string, 0, len(fxWarnings)+len(dividendWarnings)+1)
	if strings.TrimSpace(data.Warning) != "" {
		warningParts = append(warningParts, data.Warning)
	}
	warningParts = append(warningParts, dividendWarnings...)
	warningParts = append(warningParts, fxWarnings...)
	missingCurrencyWarnings := map[string]bool{}

	for _, m := range members {
		row := moexBlueChipRow{
			Ticker:         m.Ticker,
			InIndexFrom:    m.From.Format("2006-01-02"),
			InIndexTill:    m.Till.Format("2006-01-02"),
			DividendStatus: "N/A",
		}
		if div, ok := dividendInfo[strings.ToUpper(strings.TrimSpace(m.Ticker))]; ok && div.Known {
			if div.Pays {
				if div.YieldAvailable {
					row.DividendStatus = fmt.Sprintf("Да (%.2f%%)", math.Round(div.YieldPercent*100)/100)
				} else {
					row.DividendStatus = "Да (N/A)"
				}
			} else {
				row.DividendStatus = "Нет"
			}
		}
		if own, ok := byTicker[strings.ToUpper(m.Ticker)]; ok {
			row.InPortfolio = true
			row.InstrumentID = own.InstrumentID
			row.Name = own.Name
			row.Currency = own.Currency
			row.Status = own.Status
			row.OpenQty = own.OpenQty
			periodRub, okPeriod := convertNanoToRUB(own.PeriodPNLNano, own.Currency, fxRates)
			row.PeriodPNLAvailable = okPeriod
			if okPeriod {
				row.PeriodPNLNano = periodRub
				data.PeriodTotalNano += periodRub
			} else {
				code := normalizeCurrency(own.Currency)
				if code == "" {
					code = "UNKNOWN"
				}
				if !missingCurrencyWarnings[code] {
					warningParts = append(warningParts, fmt.Sprintf("Не удалось конвертировать %s -> RUB для части голубых фишек: суммы исключены из ИТОГО.", code))
					missingCurrencyWarnings[code] = true
				}
			}
			data.OwnedCount++
		}
		data.Rows = append(data.Rows, row)
	}

	sort.SliceStable(data.Rows, func(i, j int) bool {
		if data.Rows[i].InPortfolio != data.Rows[j].InPortfolio {
			return data.Rows[i].InPortfolio
		}
		return data.Rows[i].Ticker < data.Rows[j].Ticker
	})
	if len(warningParts) > 0 {
		data.Warning = strings.Join(warningParts, " ")
	}

	_ = moexBlueChipsTemplate.Execute(w, data)
}

func (a *webApp) handleInstrument(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	instrumentID, err := url.PathUnescape(strings.TrimPrefix(r.URL.Path, "/instrument/"))
	if err != nil || strings.TrimSpace(instrumentID) == "" {
		http.NotFound(w, r)
		return
	}

	fromRaw := strings.TrimSpace(r.URL.Query().Get("from"))
	toRaw := strings.TrimSpace(r.URL.Query().Get("to"))
	bounds, from, to, parseErr := parseWebBounds(fromRaw, toRaw)

	ticker, name, currency, metaErr := loadInstrumentMeta(a.db, instrumentID)

	data := detailsPageData{
		AccountID:    a.accountID,
		InstrumentID: instrumentID,
		Ticker:       ticker,
		Name:         name,
		Currency:     currency,
		From:         from,
		To:           to,
	}

	if parseErr != nil {
		data.Error = parseErr.Error()
		_ = detailsTemplate.Execute(w, data)
		return
	}
	if metaErr != nil {
		data.Error = metaErr.Error()
		_ = detailsTemplate.Execute(w, data)
		return
	}

	rows, err := loadInstrumentDeals(a.db, a.accountID, instrumentID, bounds)
	if err != nil {
		data.Error = err.Error()
		_ = detailsTemplate.Execute(w, data)
		return
	}
	data.Rows = rows
	for _, row := range rows {
		data.DealsPeriodPNLNano += row.FinalPNLNano
	}
	data.DealsLifetimePNLNano, err = loadInstrumentLifetimePNL(a.db, a.accountID, instrumentID)
	if err != nil {
		data.Error = err.Error()
		_ = detailsTemplate.Execute(w, data)
		return
	}
	data.DividendPeriodNano, data.DividendLifetimeNano, err = loadInstrumentDividendTotals(a.db, a.accountID, instrumentID, bounds)
	if err != nil {
		data.Error = err.Error()
		_ = detailsTemplate.Execute(w, data)
		return
	}
	data.Dividends, err = loadInstrumentDividends(a.db, a.accountID, instrumentID, bounds)
	if err != nil {
		data.Error = err.Error()
		_ = detailsTemplate.Execute(w, data)
		return
	}

	data.PeriodPNLNano = data.DealsPeriodPNLNano + data.DividendPeriodNano
	data.LifetimePNLNano = data.DealsLifetimePNLNano + data.DividendLifetimeNano

	_ = detailsTemplate.Execute(w, data)
}

func (a *webApp) handleOpenDeals(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	data := openDealsPageData{
		AccountID: a.accountID,
	}

	rows, err := loadOpenDeals(a.db, a.accountID)
	if err != nil {
		data.Error = err.Error()
		_ = openDealsTemplate.Execute(w, data)
		return
	}

	data.Rows = rows
	data.LotsCount = len(rows)
	agg := make(map[string]int)
	for _, row := range rows {
		data.TotalOpenQty += row.OpenQty
		data.TotalOpenCostNano += row.OpenCostNano

		idx, ok := agg[row.InstrumentID]
		if !ok {
			data.ByInstrument = append(data.ByInstrument, openDealsInstrumentSummary{
				InstrumentID: row.InstrumentID,
				Ticker:       row.Ticker,
				Name:         row.Name,
				Currency:     row.Currency,
			})
			idx = len(data.ByInstrument) - 1
			agg[row.InstrumentID] = idx
		}
		item := &data.ByInstrument[idx]
		item.LotsCount++
		item.OpenQty += row.OpenQty
		item.OpenCostNano += row.OpenCostNano
	}
	data.InstrumentsCount = len(data.ByInstrument)
	for i := range data.ByInstrument {
		if data.ByInstrument[i].OpenQty > 0 {
			data.ByInstrument[i].AvgCostNano = data.ByInstrument[i].OpenCostNano / data.ByInstrument[i].OpenQty
		}
	}
	fxRates, fxWarnings := a.loadRubFXRates(collectOpenSummaryCurrencies(data.ByInstrument))
	if len(fxWarnings) > 0 {
		if data.Warning != "" {
			data.Warning += "; "
		}
		data.Warning += strings.Join(fxWarnings, " ")
	}
	missingCurrencyWarnings := map[string]bool{}

	portfolioMap, portfolioErr := a.fetchPortfolioPositionMap()
	if portfolioErr != nil {
		if data.Warning != "" {
			data.Warning += "; "
		}
		data.Warning += "Не удалось загрузить портфель для расчета брокерского PnL: " + portfolioErr.Error()
	}
	aliasMap, aliasErr := a.loadInstrumentAliasMap()
	if aliasErr != nil {
		if data.Warning != "" {
			data.Warning += "; "
		}
		data.Warning += "не удалось загрузить алиасы инструментов: " + aliasErr.Error()
	}
	for i := range data.ByInstrument {
		item := &data.ByInstrument[i]
		pos, ok := resolvePortfolioSnapshot(item.InstrumentID, item.Ticker, aliasMap, portfolioMap)
		if !ok {
			continue
		}
		// Use broker-mode average price (non-FIFO) to match terminal PnL.
		item.BrokerAvgPriceNano = pos.AvgPriceNano
		if item.BrokerAvgPriceNano == 0 {
			item.BrokerAvgPriceNano = pos.AvgPriceFIFONano
		}
		item.BrokerCurrentPriceNano = pos.CurrentPriceNano
		if pos.Currency != "" {
			item.Currency = pos.Currency
		}
		pnlNano := int64(0)
		pnlReady := false

		// Shares: terminal-like PnL is usually (current - avg) * quantity.
		// Funds/ETFs/bonds: prefer broker-provided expected yield.
		if pos.InstrumentType == "share" && item.BrokerAvgPriceNano != 0 && item.BrokerCurrentPriceNano != 0 {
			qtyNano := pos.QuantityNano
			if qtyNano == 0 {
				qtyNano = item.OpenQty * 1_000_000_000
			}
			if v, ok := mulDivInt64Exact(item.BrokerCurrentPriceNano-item.BrokerAvgPriceNano, qtyNano, 1_000_000_000); ok {
				pnlNano = v
				pnlReady = true
			}
		}
		if !pnlReady && pos.ExpectedYieldNano != 0 {
			pnlNano = pos.ExpectedYieldNano
			pnlReady = true
		}
		if !pnlReady && pos.ExpectedYieldFIFONano != 0 {
			pnlNano = pos.ExpectedYieldFIFONano
			pnlReady = true
		}
		if !pnlReady && item.BrokerAvgPriceNano != 0 && item.BrokerCurrentPriceNano != 0 {
			qtyNano := pos.QuantityNano
			if qtyNano == 0 {
				qtyNano = item.OpenQty * 1_000_000_000
			}
			if v, ok := mulDivInt64Exact(item.BrokerCurrentPriceNano-item.BrokerAvgPriceNano, qtyNano, 1_000_000_000); ok {
				pnlNano = v
				pnlReady = true
			}
		}
		if !pnlReady {
			continue
		}
		item.BrokerDataAvailable = true
		data.BrokerDataCount++
		pnlCurrency := item.Currency
		if pos.Currency != "" {
			pnlCurrency = pos.Currency
		}
		if pnlRub, ok := convertNanoToRUB(pnlNano, pnlCurrency, fxRates); ok {
			item.BrokerPNLNano = pnlRub
			item.BrokerPNLAvailable = true
			data.TotalBrokerPNLNano += pnlRub
		} else {
			item.BrokerPNLAvailable = false
			code := normalizeCurrency(pnlCurrency)
			if code == "" {
				code = "UNKNOWN"
			}
			if !missingCurrencyWarnings[code] {
				if data.Warning != "" {
					data.Warning += "; "
				}
				data.Warning += fmt.Sprintf("Не удалось конвертировать %s -> RUB для части открытых позиций: суммы исключены из ИТОГО.", code)
				missingCurrencyWarnings[code] = true
			}
		}
	}
	all := data.ByInstrument
	data.ByInstrument = nil
	data.ByInstrumentBlocked = nil
	data.ByInstrumentUSD = nil
	data.ByInstrumentEUR = nil
	for _, item := range all {
		if isBlockedTicker(item.Ticker) {
			data.ByInstrumentBlocked = append(data.ByInstrumentBlocked, item)
			data.BlockedLotsCount += item.LotsCount
			data.BlockedOpenQty += item.OpenQty
			if item.BrokerPNLAvailable {
				data.BlockedBrokerPNLNano += item.BrokerPNLNano
			}
			continue
		}
		switch openDealsCurrencyBucket(item) {
		case "USD":
			data.ByInstrumentUSD = append(data.ByInstrumentUSD, item)
			data.USDLotsCount += item.LotsCount
			data.USDOpenQty += item.OpenQty
			if item.BrokerPNLAvailable {
				data.USDBrokerPNLNano += item.BrokerPNLNano
			}
		case "EUR":
			data.ByInstrumentEUR = append(data.ByInstrumentEUR, item)
			data.EURLotsCount += item.LotsCount
			data.EUROpenQty += item.OpenQty
			if item.BrokerPNLAvailable {
				data.EURBrokerPNLNano += item.BrokerPNLNano
			}
		default:
			data.ByInstrument = append(data.ByInstrument, item)
			data.MainLotsCount += item.LotsCount
			data.MainOpenQty += item.OpenQty
			if item.BrokerPNLAvailable {
				data.MainBrokerPNLNano += item.BrokerPNLNano
			}
		}
	}
	sortOpenSummaryByPNL(data.ByInstrument)
	sortOpenSummaryByPNL(data.ByInstrumentBlocked)
	sortOpenSummaryByPNL(data.ByInstrumentUSD)
	sortOpenSummaryByPNL(data.ByInstrumentEUR)

	_ = openDealsTemplate.Execute(w, data)
}

type portfolioPositionSnapshot struct {
	AvgPriceNano          int64
	AvgPriceFIFONano      int64
	CurrentPriceNano      int64
	QuantityNano          int64
	ExpectedYieldNano     int64
	ExpectedYieldFIFONano int64
	InstrumentType        string
	Currency              string
}

func (a *webApp) fetchPortfolioPositionMap() (map[string]portfolioPositionSnapshot, error) {
	out := make(map[string]portfolioPositionSnapshot)
	if a.api == nil || a.api.operations == nil {
		return out, fmt.Errorf("operations client is not initialized")
	}

	ctx, cancel := requestContext(a.cfg.appName, a.cfg.timeout)
	resp, err := a.api.operations.GetPortfolio(ctx, &pb.PortfolioRequest{AccountId: a.accountID})
	cancel()
	if err != nil {
		return out, err
	}

	for _, p := range resp.GetPositions() {
		snap := portfolioPositionSnapshot{
			AvgPriceNano:          moneyValueToNano(p.GetAveragePositionPrice()),
			AvgPriceFIFONano:      moneyValueToNano(p.GetAveragePositionPriceFifo()),
			CurrentPriceNano:      moneyValueToNano(p.GetCurrentPrice()),
			QuantityNano:          quotationToNano(p.GetQuantity()),
			ExpectedYieldNano:     quotationToNano(p.GetExpectedYield()),
			ExpectedYieldFIFONano: quotationToNano(p.GetExpectedYieldFifo()),
			InstrumentType:        strings.ToLower(strings.TrimSpace(p.GetInstrumentType())),
			Currency:              portfolioPositionCurrency(p),
		}
		if uid := strings.TrimSpace(p.GetInstrumentUid()); uid != "" {
			out[uid] = snap
			out[strings.ToUpper(uid)] = snap
		}
		if figi := strings.TrimSpace(p.GetFigi()); figi != "" {
			out[figi] = snap
			out[strings.ToUpper(figi)] = snap
		}
		if a.api != nil && a.api.instruments != nil {
			idType := pb.InstrumentIdType_INSTRUMENT_ID_TYPE_UID
			id := strings.TrimSpace(p.GetInstrumentUid())
			if id == "" {
				idType = pb.InstrumentIdType_INSTRUMENT_ID_TYPE_FIGI
				id = strings.TrimSpace(p.GetFigi())
			}
			if id != "" {
				ctx2, cancel2 := requestContext(a.cfg.appName, a.cfg.timeout)
				ir, err := a.api.instruments.GetInstrumentBy(ctx2, &pb.InstrumentRequest{IdType: idType, Id: id})
				cancel2()
				if err == nil && ir.GetInstrument() != nil {
					t := strings.ToUpper(strings.TrimSpace(ir.GetInstrument().GetTicker()))
					if t != "" {
						out["TICKER:"+t] = snap
					}
				}
			}
		}
	}

	return out, nil
}

func (a *webApp) loadInstrumentAliasMap() (map[string][]string, error) {
	rows, err := a.db.Query(`
		SELECT COALESCE(instrument_id, ''), COALESCE(figi, ''), COALESCE(instrument_uid, '')
		FROM instruments
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string][]string)
	for rows.Next() {
		var instrumentID, figi, uid string
		if err := rows.Scan(&instrumentID, &figi, &uid); err != nil {
			return nil, err
		}
		instrumentID = strings.TrimSpace(instrumentID)
		if instrumentID == "" {
			continue
		}
		aliases := []string{instrumentID}
		if figi = strings.TrimSpace(figi); figi != "" {
			aliases = append(aliases, figi)
		}
		if uid = strings.TrimSpace(uid); uid != "" {
			aliases = append(aliases, uid)
		}
		out[instrumentID] = aliases
		out[strings.ToUpper(instrumentID)] = aliases
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func resolvePortfolioSnapshot(instrumentID, ticker string, aliasMap map[string][]string, portfolioMap map[string]portfolioPositionSnapshot) (portfolioPositionSnapshot, bool) {
	aliases := []string{strings.TrimSpace(instrumentID)}
	if v, ok := aliasMap[strings.TrimSpace(instrumentID)]; ok && len(v) > 0 {
		aliases = v
	} else if v, ok := aliasMap[strings.ToUpper(strings.TrimSpace(instrumentID))]; ok && len(v) > 0 {
		aliases = v
	}

	for _, k := range aliases {
		k = strings.TrimSpace(k)
		if k == "" {
			continue
		}
		if snap, ok := portfolioMap[k]; ok {
			return snap, true
		}
		if snap, ok := portfolioMap[strings.ToUpper(k)]; ok {
			return snap, true
		}
	}
	if t := strings.ToUpper(strings.TrimSpace(ticker)); t != "" {
		if snap, ok := portfolioMap["TICKER:"+t]; ok {
			return snap, true
		}
	}

	return portfolioPositionSnapshot{}, false
}

type moexISSTable struct {
	Columns []string        `json:"columns"`
	Data    [][]interface{} `json:"data"`
}

type moexMOEXBCTickersResponse struct {
	Tickers moexISSTable `json:"tickers"`
}

type moexBlueChipIndexMember struct {
	Ticker string
	From   time.Time
	Till   time.Time
}

type blueChipDividendInfo struct {
	Known          bool
	Pays           bool
	YieldPercent   float64
	YieldAvailable bool
}

func (a *webApp) fetchMOEXBCActiveMembers(asOf time.Time) ([]moexBlueChipIndexMember, time.Time, error) {
	const endpoint = "https://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/MOEXBC/tickers.json?iss.meta=off"

	client := &http.Client{Timeout: a.cfg.timeout}
	resp, err := client.Get(endpoint)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, time.Time{}, fmt.Errorf("http status %s", resp.Status)
	}

	var payload moexMOEXBCTickersResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, time.Time{}, err
	}
	if len(payload.Tickers.Columns) == 0 {
		return nil, time.Time{}, fmt.Errorf("empty tickers columns in ISS response")
	}

	tickerIdx := moexISSColumnIndex(payload.Tickers.Columns, "ticker")
	fromIdx := moexISSColumnIndex(payload.Tickers.Columns, "from")
	tillIdx := moexISSColumnIndex(payload.Tickers.Columns, "till")
	if tickerIdx < 0 || fromIdx < 0 || tillIdx < 0 {
		return nil, time.Time{}, fmt.Errorf("missing required columns (ticker/from/till)")
	}

	asOfDate := time.Date(asOf.UTC().Year(), asOf.UTC().Month(), asOf.UTC().Day(), 0, 0, 0, 0, time.UTC)
	type parsedMember struct {
		ticker string
		from   time.Time
		till   time.Time
	}
	parsed := make([]parsedMember, 0, len(payload.Tickers.Data))
	latestTill := time.Time{}

	for _, row := range payload.Tickers.Data {
		ticker := strings.ToUpper(strings.TrimSpace(moexISSCellString(row, tickerIdx)))
		if ticker == "" {
			continue
		}
		fromDate, errFrom := moexISSParseDate(moexISSCellString(row, fromIdx))
		tillDate, errTill := moexISSParseDate(moexISSCellString(row, tillIdx))
		if errFrom != nil || errTill != nil {
			continue
		}
		parsed = append(parsed, parsedMember{ticker: ticker, from: fromDate, till: tillDate})
		if tillDate.After(latestTill) {
			latestTill = tillDate
		}
	}

	filterAt := func(targetDate time.Time) []moexBlueChipIndexMember {
		uniq := map[string]moexBlueChipIndexMember{}
		for _, m := range parsed {
			if m.from.After(targetDate) || m.till.Before(targetDate) {
				continue
			}
			member := moexBlueChipIndexMember{
				Ticker: m.ticker,
				From:   m.from,
				Till:   m.till,
			}
			if prev, exists := uniq[m.ticker]; !exists || member.Till.After(prev.Till) {
				uniq[m.ticker] = member
			}
		}
		out := make([]moexBlueChipIndexMember, 0, len(uniq))
		for _, member := range uniq {
			out = append(out, member)
		}
		sort.SliceStable(out, func(i, j int) bool { return out[i].Ticker < out[j].Ticker })
		return out
	}

	out := filterAt(asOfDate)
	effectiveDate := asOfDate
	if len(out) == 0 && !latestTill.IsZero() {
		out = filterAt(latestTill)
		effectiveDate = latestTill
	}
	return out, effectiveDate, nil
}

type blueChipShareMeta struct {
	figi           string
	classCode      string
	apiTrade       bool
	buyAvailable   bool
	sellAvailable  bool
	blockedTCA     bool
	otc            bool
	dividendPaying bool
}

func (a *webApp) loadBlueChipDividendInfo(members []moexBlueChipIndexMember) (map[string]blueChipDividendInfo, []string) {
	out := make(map[string]blueChipDividendInfo, len(members))
	if len(members) == 0 {
		return out, nil
	}
	if a.api == nil || a.api.instruments == nil {
		return out, []string{"Не удалось получить дивидендные признаки: instruments client не инициализирован."}
	}

	ctx, cancel := requestContext(a.cfg.appName, a.cfg.timeout)
	resp, err := a.api.instruments.Shares(ctx, &pb.InstrumentsRequest{
		InstrumentStatus: pb.InstrumentStatus_INSTRUMENT_STATUS_ALL,
	})
	cancel()
	if err != nil {
		return out, []string{"Не удалось получить список акций для дивидендных признаков: " + err.Error()}
	}

	byTicker := make(map[string]blueChipShareMeta)
	for _, sh := range resp.GetInstruments() {
		ticker := strings.ToUpper(strings.TrimSpace(sh.GetTicker()))
		if ticker == "" {
			continue
		}
		candidate := blueChipShareMeta{
			figi:           strings.TrimSpace(sh.GetFigi()),
			classCode:      strings.ToUpper(strings.TrimSpace(sh.GetClassCode())),
			apiTrade:       sh.GetApiTradeAvailableFlag(),
			buyAvailable:   sh.GetBuyAvailableFlag(),
			sellAvailable:  sh.GetSellAvailableFlag(),
			blockedTCA:     sh.GetBlockedTcaFlag(),
			otc:            sh.GetOtcFlag(),
			dividendPaying: sh.GetDivYieldFlag(),
		}
		prev, exists := byTicker[ticker]
		if !exists || scoreBlueChipShareMeta(candidate) > scoreBlueChipShareMeta(prev) {
			byTicker[ticker] = candidate
		}
	}

	yieldFailed := map[string]bool{}
	for _, m := range members {
		ticker := strings.ToUpper(strings.TrimSpace(m.Ticker))
		if ticker == "" {
			continue
		}
		meta, ok := byTicker[ticker]
		if !ok {
			continue
		}

		info := blueChipDividendInfo{
			Known: true,
			Pays:  meta.dividendPaying,
		}
		if info.Pays && meta.figi != "" {
			y, okYield, err := a.fetchLatestDividendYieldPercent(meta.figi)
			if err != nil {
				yieldFailed[ticker] = true
			} else if okYield {
				info.YieldPercent = y
				info.YieldAvailable = true
			}
		}
		out[ticker] = info
	}

	if len(yieldFailed) == 0 {
		return out, nil
	}

	failed := make([]string, 0, len(yieldFailed))
	for t := range yieldFailed {
		failed = append(failed, t)
	}
	sort.Strings(failed)
	if len(failed) > 8 {
		head := strings.Join(failed[:8], ", ")
		return out, []string{fmt.Sprintf("Не удалось получить процент дивдоходности для части голубых фишек: %s и еще %d.", head, len(failed)-8)}
	}
	return out, []string{"Не удалось получить процент дивдоходности для части голубых фишек: " + strings.Join(failed, ", ")}
}

func scoreBlueChipShareMeta(m blueChipShareMeta) int {
	score := 0
	switch m.classCode {
	case "TQBR":
		score += 150
	case "TQTF":
		score += 80
	case "FQBR":
		score += 60
	}
	if m.apiTrade {
		score += 20
	}
	if m.buyAvailable {
		score += 10
	}
	if m.sellAvailable {
		score += 8
	}
	if m.blockedTCA {
		score -= 25
	}
	if m.otc {
		score -= 15
	}
	if m.figi != "" {
		score++
	}
	return score
}

func (a *webApp) fetchLatestDividendYieldPercent(figi string) (float64, bool, error) {
	figi = strings.TrimSpace(figi)
	if figi == "" {
		return 0, false, nil
	}
	if a.api == nil || a.api.instruments == nil {
		return 0, false, fmt.Errorf("instruments client is not initialized")
	}

	now := time.Now().UTC()
	ctx, cancel := requestContext(a.cfg.appName, a.cfg.timeout)
	resp, err := a.api.instruments.GetDividends(ctx, &pb.GetDividendsRequest{
		Figi: figi,
		From: timestamppb.New(now.AddDate(-5, 0, 0)),
		To:   timestamppb.New(now.AddDate(1, 0, 0)),
	})
	cancel()
	if err != nil {
		return 0, false, err
	}

	var (
		bestYield float64
		bestDate  time.Time
		found     bool
	)
	for _, d := range resp.GetDividends() {
		y := quotationToFloat64(d.GetYieldValue())
		if y == 0 {
			continue
		}
		dt := dividendEventDate(d)
		if !found || dt.After(bestDate) {
			bestDate = dt
			bestYield = y
			found = true
		}
	}
	return bestYield, found, nil
}

func dividendEventDate(d *pb.Dividend) time.Time {
	if d == nil {
		return time.Time{}
	}
	candidates := []time.Time{
		d.GetRecordDate().AsTime().UTC(),
		d.GetPaymentDate().AsTime().UTC(),
		d.GetLastBuyDate().AsTime().UTC(),
		d.GetDeclaredDate().AsTime().UTC(),
		d.GetCreatedAt().AsTime().UTC(),
	}
	for _, t := range candidates {
		if !t.IsZero() {
			return t
		}
	}
	return time.Time{}
}

func moexISSColumnIndex(columns []string, name string) int {
	target := strings.ToLower(strings.TrimSpace(name))
	for i, col := range columns {
		if strings.ToLower(strings.TrimSpace(col)) == target {
			return i
		}
	}
	return -1
}

func moexISSCellString(row []interface{}, idx int) string {
	if idx < 0 || idx >= len(row) {
		return ""
	}
	switch v := row[idx].(type) {
	case string:
		return strings.TrimSpace(v)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", v))
	}
}

func moexISSParseDate(v string) (time.Time, error) {
	return time.ParseInLocation("2006-01-02", strings.TrimSpace(v), time.UTC)
}

func collectOverviewCurrencies(rows []instrumentOverview) []string {
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

func collectOpenSummaryCurrencies(rows []openDealsInstrumentSummary) []string {
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

func normalizeCurrency(v string) string {
	code := strings.ToUpper(strings.TrimSpace(v))
	switch code {
	case "RUR":
		return "RUB"
	default:
		return code
	}
}

func historicalCurrencyBucket(row instrumentOverview) string {
	currency := normalizeCurrency(row.Currency)
	if currency == "USD" || currency == "EUR" {
		return currency
	}

	ticker := strings.ToUpper(strings.TrimSpace(row.Ticker))
	name := strings.ToUpper(strings.TrimSpace(row.Name))
	id := strings.ToUpper(strings.TrimSpace(row.InstrumentID))

	if ticker == "USD" ||
		strings.HasPrefix(ticker, "USD000") ||
		strings.HasPrefix(ticker, "USDRUB") ||
		strings.Contains(name, "ДОЛЛАР") ||
		id == "BBG0013HGFT4" {
		return "USD"
	}
	if ticker == "EUR" ||
		strings.HasPrefix(ticker, "EUR000") ||
		strings.HasPrefix(ticker, "EURRUB") ||
		strings.Contains(name, "ЕВРО") ||
		id == "BBG0013HJJ31" {
		return "EUR"
	}
	return ""
}

func openDealsCurrencyBucket(item openDealsInstrumentSummary) string {
	currency := normalizeCurrency(item.Currency)
	if currency == "USD" || currency == "EUR" {
		return currency
	}

	ticker := strings.ToUpper(strings.TrimSpace(item.Ticker))
	name := strings.ToUpper(strings.TrimSpace(item.Name))
	id := strings.ToUpper(strings.TrimSpace(item.InstrumentID))
	if ticker == "USD" ||
		strings.HasPrefix(ticker, "USD000") ||
		strings.HasPrefix(ticker, "USDRUB") ||
		strings.Contains(name, "ДОЛЛАР") ||
		id == "BBG0013HGFT4" {
		return "USD"
	}
	if ticker == "EUR" ||
		strings.HasPrefix(ticker, "EUR000") ||
		strings.HasPrefix(ticker, "EURRUB") ||
		strings.Contains(name, "ЕВРО") ||
		id == "BBG0013HJJ31" {
		return "EUR"
	}
	return ""
}

func isBlockedInstrument(row instrumentOverview) bool {
	return isBlockedTicker(row.Ticker)
}

func isBlockedTicker(ticker string) bool {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	switch ticker {
	case "FIXR", "POGR", "TIPO", "TFNX":
		return true
	default:
		return false
	}
}

func sortOverviewByPNL(rows []instrumentOverview) {
	sort.SliceStable(rows, func(i, j int) bool {
		pi := overviewSortPnL(rows[i])
		pj := overviewSortPnL(rows[j])
		if pi != pj {
			return pi > pj
		}
		if rows[i].Ticker == rows[j].Ticker {
			return rows[i].InstrumentID < rows[j].InstrumentID
		}
		return rows[i].Ticker < rows[j].Ticker
	})
}

func sortOpenSummaryByPNL(rows []openDealsInstrumentSummary) {
	sort.SliceStable(rows, func(i, j int) bool {
		pi := openSummarySortPnL(rows[i])
		pj := openSummarySortPnL(rows[j])
		if pi != pj {
			return pi > pj
		}
		if rows[i].Ticker == rows[j].Ticker {
			return rows[i].InstrumentID < rows[j].InstrumentID
		}
		return rows[i].Ticker < rows[j].Ticker
	})
}

func overviewSortPnL(row instrumentOverview) int64 {
	if row.PeriodPNLAvailable {
		return row.PeriodPNLNano
	}
	return -1 << 63
}

func openSummarySortPnL(row openDealsInstrumentSummary) int64 {
	if row.BrokerPNLAvailable {
		return row.BrokerPNLNano
	}
	return -1 << 63
}

func convertNanoToRUB(amountNano int64, currency string, rubRates map[string]int64) (int64, bool) {
	code := normalizeCurrency(currency)
	if code == "" {
		return 0, false
	}
	if code == "RUB" {
		return amountNano, true
	}
	rateNano, ok := rubRates[code]
	if !ok || rateNano <= 0 {
		return 0, false
	}
	return mulDivInt64Exact(amountNano, rateNano, 1_000_000_000)
}

func (a *webApp) loadRubFXRates(currencies []string) (map[string]int64, []string) {
	rates := map[string]int64{
		"RUB": 1_000_000_000,
	}
	if len(currencies) == 0 {
		return rates, nil
	}

	var warnings []string
	for _, rawCode := range currencies {
		code := normalizeCurrency(rawCode)
		if code == "" || code == "RUB" {
			continue
		}
		rateNano, brokerErr := a.fetchRubFXRateNano(code)
		cbrRateNano, cbrErr := a.fetchCBRRubFXRateNano(code)

		// For USD/EUR prefer CBR rate to keep RUB conversion stable and avoid
		// possible mismatches from broker FX instrument search.
		if isCBRPreferredFXCode(code) && cbrErr == nil && cbrRateNano > 0 {
			rates[code] = cbrRateNano
			continue
		}
		if brokerErr == nil && rateNano > 0 {
			rates[code] = rateNano
			continue
		}
		if cbrErr == nil && cbrRateNano > 0 {
			rates[code] = cbrRateNano
			continue
		}

		switch {
		case brokerErr != nil && cbrErr != nil:
			warnings = append(warnings, fmt.Sprintf("Не удалось получить FX курс %s/RUB: брокер=%v; ЦБ=%v", code, brokerErr, cbrErr))
		case brokerErr != nil:
			warnings = append(warnings, fmt.Sprintf("Не удалось получить FX курс %s/RUB: брокер=%v", code, brokerErr))
		case cbrErr != nil:
			warnings = append(warnings, fmt.Sprintf("Не удалось получить FX курс %s/RUB: брокер=пустая цена; ЦБ=%v", code, cbrErr))
		default:
			warnings = append(warnings, fmt.Sprintf("Не удалось получить FX курс %s/RUB: брокер=пустая цена; ЦБ=пустая цена.", code))
		}
	}

	return rates, warnings
}

func isCBRPreferredFXCode(code string) bool {
	switch normalizeCurrency(code) {
	case "USD", "EUR":
		return true
	default:
		return false
	}
}

type cbrDailyRates struct {
	Valutes []cbrDailyValute `xml:"Valute"`
}

type cbrDailyValute struct {
	CharCode string `xml:"CharCode"`
	Nominal  string `xml:"Nominal"`
	Value    string `xml:"Value"`
}

func (a *webApp) fetchCBRRubFXRateNano(currencyCode string) (int64, error) {
	code := normalizeCurrency(currencyCode)
	if code == "RUB" {
		return 1_000_000_000, nil
	}
	if code == "" {
		return 0, fmt.Errorf("empty currency code")
	}

	client := &http.Client{Timeout: a.cfg.timeout}
	resp, err := client.Get("https://www.cbr.ru/scripts/XML_daily.asp")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("http status %s", resp.Status)
	}

	var daily cbrDailyRates
	if err := xml.NewDecoder(resp.Body).Decode(&daily); err != nil {
		return 0, err
	}

	for _, item := range daily.Valutes {
		if normalizeCurrency(item.CharCode) != code {
			continue
		}
		nominal, err := strconv.ParseInt(strings.TrimSpace(item.Nominal), 10, 64)
		if err != nil || nominal <= 0 {
			return 0, fmt.Errorf("invalid nominal %q for %s", item.Nominal, code)
		}
		valueNano, err := parseCBRDecimalToNano(item.Value)
		if err != nil {
			return 0, fmt.Errorf("invalid value %q for %s: %w", item.Value, code, err)
		}
		rateNano, ok := mulDivInt64Exact(valueNano, 1, nominal)
		if !ok || rateNano <= 0 {
			return 0, fmt.Errorf("invalid rate math for %s", code)
		}
		return rateNano, nil
	}

	return 0, fmt.Errorf("currency %s not found in cbr feed", code)
}

func parseCBRDecimalToNano(raw string) (int64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, fmt.Errorf("empty decimal")
	}
	s = strings.ReplaceAll(s, ",", ".")

	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	if v <= 0 {
		return 0, fmt.Errorf("non-positive decimal")
	}

	nano := v * 1_000_000_000
	if nano > float64(math.MaxInt64) {
		return 0, fmt.Errorf("decimal overflow")
	}
	return int64(math.Round(nano)), nil
}

func (a *webApp) fetchRubFXRateNano(currencyCode string) (int64, error) {
	code := normalizeCurrency(currencyCode)
	if code == "RUB" {
		return 1_000_000_000, nil
	}
	if code == "" {
		return 0, fmt.Errorf("empty currency code")
	}
	if a.api == nil || a.api.instruments == nil || a.api.marketData == nil {
		return 0, fmt.Errorf("api clients are not initialized")
	}

	idsToTry := make([]string, 0, 2)
	if knownID := knownRubFXInstrumentID(code); knownID != "" {
		idsToTry = append(idsToTry, knownID)
	}
	if id, err := a.findCurrencyPairInstrumentID(code); err == nil && id != "" {
		already := false
		for _, x := range idsToTry {
			if x == id {
				already = true
				break
			}
		}
		if !already {
			idsToTry = append(idsToTry, id)
		}
	}
	if len(idsToTry) == 0 {
		return 0, fmt.Errorf("instrument for %s/RUB not found", code)
	}

	var lastErr error
	for _, id := range idsToTry {
		priceNano, err := a.fetchLastPriceNanoByInstrumentID(id)
		if err == nil && priceNano > 0 {
			return priceNano, nil
		}
		if err != nil {
			lastErr = err
			continue
		}
		lastErr = fmt.Errorf("empty last price for %s/RUB instrument %s", code, id)
	}
	if lastErr != nil {
		return 0, lastErr
	}
	return 0, fmt.Errorf("empty last price for %s/RUB", code)
}

func (a *webApp) fetchLastPriceNanoByInstrumentID(instrumentID string) (int64, error) {
	id := strings.TrimSpace(instrumentID)
	if id == "" {
		return 0, fmt.Errorf("empty instrument id")
	}

	ctx, cancel := requestContext(a.cfg.appName, a.cfg.timeout)
	resp, err := a.api.marketData.GetLastPrices(ctx, &pb.GetLastPricesRequest{
		InstrumentId: []string{id},
	})
	cancel()
	if err != nil {
		return 0, err
	}
	for _, lp := range resp.GetLastPrices() {
		priceNano := quotationToNano(lp.GetPrice())
		if priceNano > 0 {
			return priceNano, nil
		}
	}
	return 0, nil
}

func (a *webApp) findCurrencyPairInstrumentID(currencyCode string) (string, error) {
	code := normalizeCurrency(currencyCode)
	queries := fxSearchQueries(code)
	var lastErr error

	for _, tradeAvailable := range []bool{true, false} {
		for _, q := range queries {
			ctx, cancel := requestContext(a.cfg.appName, a.cfg.timeout)
			resp, err := a.api.instruments.FindInstrument(ctx, &pb.FindInstrumentRequest{
				Query:                 q,
				InstrumentKind:        pb.InstrumentType_INSTRUMENT_TYPE_CURRENCY,
				ApiTradeAvailableFlag: tradeAvailable,
			})
			cancel()
			if err != nil {
				lastErr = err
				continue
			}
			id := chooseBestFXInstrumentID(resp.GetInstruments(), code)
			if id != "" {
				return id, nil
			}
		}
	}

	if knownID := knownRubFXInstrumentID(code); knownID != "" {
		return knownID, nil
	}

	if lastErr != nil {
		return "", fmt.Errorf("instrument search error for %s/RUB: %w", code, lastErr)
	}
	return "", fmt.Errorf("instrument for %s/RUB not found", code)
}

func fxSearchQueries(currencyCode string) []string {
	code := normalizeCurrency(currencyCode)
	candidates := []string{
		code + "RUB",
		code + "/RUB",
		code + "000UTSTOM",
		code + "000000TOD",
		code + "RUBF",
	}
	switch code {
	case "USD":
		candidates = append(candidates, "USDRUB", "USD000UTSTOM", "USDRUBF")
	case "EUR":
		candidates = append(candidates, "EURRUB", "EUR_RUB__TOM", "EURRUBF")
	case "CNY":
		candidates = append(candidates, "CNYRUB", "CNYRUB_TOM", "CNYRUBF")
	case "HKD":
		candidates = append(candidates, "HKDRUB", "HKDRUB_TOM", "HKDRUBF")
	}

	out := make([]string, 0, len(candidates))
	seen := map[string]bool{}
	for _, q := range candidates {
		q = strings.TrimSpace(q)
		if q == "" || seen[q] {
			continue
		}
		seen[q] = true
		out = append(out, q)
	}
	return out
}

func knownRubFXInstrumentID(currencyCode string) string {
	switch normalizeCurrency(currencyCode) {
	case "USD":
		return "BBG0013HGFT4"
	case "EUR":
		return "BBG0013HJJ31"
	default:
		return ""
	}
}

func chooseBestFXInstrumentID(items []*pb.InstrumentShort, currencyCode string) string {
	code := normalizeCurrency(currencyCode)
	bestScore := -1
	bestID := ""
	for _, it := range items {
		if it == nil {
			continue
		}
		score := scoreFXInstrument(it, code)
		if score <= bestScore {
			continue
		}
		id := strings.TrimSpace(it.GetUid())
		if id == "" {
			id = strings.TrimSpace(it.GetFigi())
		}
		if id == "" {
			continue
		}
		bestScore = score
		bestID = id
	}
	return bestID
}

func scoreFXInstrument(it *pb.InstrumentShort, currencyCode string) int {
	ticker := strings.ToUpper(strings.TrimSpace(it.GetTicker()))
	name := strings.ToUpper(strings.TrimSpace(it.GetName()))
	classCode := strings.ToUpper(strings.TrimSpace(it.GetClassCode()))

	score := 0
	if ticker == currencyCode+"000UTSTOM" {
		score += 1000
	}
	if ticker == currencyCode+"000000TOD" {
		score += 950
	}
	if strings.Contains(ticker, currencyCode) && strings.Contains(ticker, "RUB") {
		score += 500
	}
	if strings.Contains(name, currencyCode) && strings.Contains(name, "RUB") {
		score += 300
	}
	if classCode == "CETS" {
		score += 120
	}
	if strings.Contains(ticker, "TOM") {
		score += 80
	}
	if strings.Contains(ticker, "TOD") {
		score += 60
	}
	if it.GetApiTradeAvailableFlag() {
		score += 20
	}
	if strings.HasSuffix(ticker, "F") {
		score -= 20
	}
	return score
}

func portfolioPositionCurrency(p *pb.PortfolioPosition) string {
	if p == nil {
		return ""
	}
	candidates := []string{
		normalizeCurrency(p.GetAveragePositionPrice().GetCurrency()),
		normalizeCurrency(p.GetCurrentPrice().GetCurrency()),
		normalizeCurrency(p.GetAveragePositionPriceFifo().GetCurrency()),
	}
	for _, code := range candidates {
		if code != "" {
			return code
		}
	}
	return ""
}

func quotationToNano(v *pb.Quotation) int64 {
	if v == nil {
		return 0
	}
	return v.GetUnits()*1_000_000_000 + int64(v.GetNano())
}

func quotationToFloat64(v *pb.Quotation) float64 {
	if v == nil {
		return 0
	}
	return float64(v.GetUnits()) + float64(v.GetNano())/1e9
}

func mulDivInt64Exact(a, b, c int64) (int64, bool) {
	if c == 0 {
		return 0, false
	}
	x := big.NewInt(a)
	x.Mul(x, big.NewInt(b))
	x.Div(x, big.NewInt(c))
	if !x.IsInt64() {
		return 0, false
	}
	return x.Int64(), true
}

func parseWebBounds(fromRaw, toRaw string) (periodBounds, string, string, error) {
	now := time.Now().UTC()
	defaultFrom := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	defaultTo := now

	from := defaultFrom
	to := defaultTo
	var err error

	if fromRaw != "" {
		from, err = parseDateOrTimestamp(fromRaw, false)
		if err != nil {
			return periodBounds{}, fromRaw, toRaw, fmt.Errorf("invalid from: %w", err)
		}
	}
	if toRaw != "" {
		to, err = parseDateOrTimestamp(toRaw, true)
		if err != nil {
			return periodBounds{}, fromRaw, toRaw, fmt.Errorf("invalid to: %w", err)
		}
	}
	if !from.Before(to) {
		return periodBounds{}, fromRaw, toRaw, fmt.Errorf("period must satisfy from < to")
	}

	return periodBounds{from: from.UTC(), to: to.UTC()}, from.Format("2006-01-02"), to.Format("2006-01-02"), nil
}

func formatNano(v int64) string {
	neg := v < 0
	if neg {
		v = -v
	}

	// Round nano amount to kopecks (2 decimal places).
	kopecks := (v + 5_000_000) / 10_000_000
	if kopecks == 0 {
		neg = false
	}

	sign := ""
	if neg {
		sign = "-"
	}
	return fmt.Sprintf("%s%d.%02d", sign, kopecks/100, kopecks%100)
}

func formatTimeShort(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}
