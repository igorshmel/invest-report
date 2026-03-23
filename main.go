package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	pb "github.com/tinkoff/invest-api-go-sdk/proto"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultProdEndpoint    = "invest-public-api.tbank.ru:443"
	defaultSandboxEndpoint = "sandbox-invest-public-api.tbank.ru:443"
	defaultAppName         = "tinvest-pnl-report"
)

type config struct {
	mode               string
	token              string
	accountID          string
	fromRaw            string
	toRaw              string
	endpoint           string
	appName            string
	caCertFile         string
	dbPath             string
	httpAddr           string
	sandbox            bool
	listAccounts       bool
	syncOnStart        bool
	insecureSkipVerify bool
	syncForecasts      bool
	evaluateForecasts  bool
	syncAll            bool
	forecastProvider   string
	forecastFallback   string
	forecastHorizons   string
	forecastInterval   time.Duration
	forecastFallbackOn bool
	finnhubAPIKey      string
	timeout            time.Duration
}

type lot struct {
	qty      int64
	unitCost float64
}

type positionState struct {
	lots []lot
}

type instrumentStat struct {
	figi        string
	ticker      string
	name        string
	currency    string
	boughtQty   int64
	soldQty     int64
	buyAmount   float64
	sellAmount  float64
	realizedPnL float64
	dividendPnL float64
	openQty     int64
	openCost    float64
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		exitWithError(err)
	}

	if cfg.token == "" {
		exitWithError(errors.New("token is required: pass -token or set TINVEST_TOKEN"))
	}

	if cfg.endpoint == "" {
		if cfg.sandbox {
			cfg.endpoint = defaultSandboxEndpoint
		} else {
			cfg.endpoint = defaultProdEndpoint
		}
	}

	tlsConfig, err := buildTLSConfig(cfg)
	if err != nil {
		exitWithError(err)
	}
	if cfg.insecureSkipVerify {
		fmt.Fprintln(os.Stderr, "warning: TLS certificate verification is disabled (-insecure-skip-verify)")
	}

	conn, err := grpc.Dial(
		cfg.endpoint,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		grpc.WithPerRPCCredentials(oauth.TokenSource{
			TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: cfg.token}),
		}),
	)
	if err != nil {
		exitWithError(fmt.Errorf("dial t-invest endpoint: %w", err))
	}
	defer conn.Close()

	usersClient := pb.NewUsersServiceClient(conn)
	operationsClient := pb.NewOperationsServiceClient(conn)
	instrumentsClient := pb.NewInstrumentsServiceClient(conn)
	marketDataClient := pb.NewMarketDataServiceClient(conn)

	switch cfg.mode {
	case "sync", "serve", "sync-forecasts", "evaluate-forecasts", "sync-all":
		if cfg.listAccounts {
			accounts, err := getAccounts(usersClient, cfg.appName, cfg.timeout)
			if err != nil {
				exitWithError(fmt.Errorf("get accounts: %w", err))
			}
			printAccounts(accounts)
			return
		}

		if cfg.accountID == "" {
			accounts, err := getAccounts(usersClient, cfg.appName, cfg.timeout)
			if err != nil {
				exitWithError(fmt.Errorf("get accounts: %w", err))
			}
			cfg.accountID, err = resolveAccountID("", accounts)
			if err != nil {
				exitWithError(err)
			}
		}

		db, err := openSQLite(cfg.dbPath)
		if err != nil {
			exitWithError(fmt.Errorf("open db: %w", err))
		}
		defer db.Close()

		if err := initSchema(db); err != nil {
			exitWithError(fmt.Errorf("init schema: %w", err))
		}

		api := &apiClients{
			operations:  operationsClient,
			instruments: instrumentsClient,
			marketData:  marketDataClient,
		}

		if cfg.mode == "sync" {
			if err := syncAndRebuild(db, api, cfg, cfg.accountID); err != nil {
				exitWithError(err)
			}
			fmt.Printf("Sync completed for account %s\n", cfg.accountID)
			return
		}
		if cfg.mode == "sync-forecasts" {
			stats, err := syncForecastSnapshots(db, api, cfg, cfg.accountID, time.Now().UTC())
			if err != nil {
				exitWithError(err)
			}
			for _, w := range stats.Warnings {
				fmt.Fprintf(os.Stderr, "warning: %s\n", w)
			}
			fmt.Printf("Forecast sync completed: snapshots=%d providers=%d skipped=%d account=%s\n", stats.SnapshotsInserted, stats.ProviderRowsInserted, stats.InstrumentsSkipped, cfg.accountID)
			return
		}
		if cfg.mode == "evaluate-forecasts" {
			stats, err := evaluateForecastBacktests(db, api, cfg, cfg.accountID, time.Now().UTC())
			if err != nil {
				exitWithError(err)
			}
			for _, w := range stats.Warnings {
				fmt.Fprintf(os.Stderr, "warning: %s\n", w)
			}
			fmt.Printf("Forecast backtest completed: inserted=%d skipped=%d account=%s\n", stats.RowsInserted, stats.RowsSkipped, cfg.accountID)
			return
		}
		if cfg.mode == "sync-all" {
			if err := syncAndRebuild(db, api, cfg, cfg.accountID); err != nil {
				exitWithError(err)
			}
			syncStats, err := syncForecastSnapshots(db, api, cfg, cfg.accountID, time.Now().UTC())
			if err != nil {
				exitWithError(err)
			}
			evalStats, err := evaluateForecastBacktests(db, api, cfg, cfg.accountID, time.Now().UTC())
			if err != nil {
				exitWithError(err)
			}
			for _, w := range syncStats.Warnings {
				fmt.Fprintf(os.Stderr, "warning: %s\n", w)
			}
			for _, w := range evalStats.Warnings {
				fmt.Fprintf(os.Stderr, "warning: %s\n", w)
			}
			fmt.Printf("Sync-all completed: snapshots=%d providers=%d evaluated=%d account=%s\n", syncStats.SnapshotsInserted, syncStats.ProviderRowsInserted, evalStats.RowsInserted, cfg.accountID)
			return
		}

		if cfg.syncOnStart {
			if err := syncAndRebuild(db, api, cfg, cfg.accountID); err != nil {
				fmt.Fprintf(os.Stderr, "warning: initial sync failed: %v\n", err)
			}
		}

		if err := runWebServer(db, api, cfg, cfg.accountID); err != nil {
			exitWithError(err)
		}
		return
	case "report":
		// legacy direct report mode
	default:
		exitWithError(fmt.Errorf("unsupported -mode %q (expected report|sync|serve|sync-forecasts|evaluate-forecasts|sync-all)", cfg.mode))
	}

	var accounts []*pb.Account
	if cfg.listAccounts || cfg.accountID == "" {
		accounts, err = getAccounts(usersClient, cfg.appName, cfg.timeout)
		if err != nil {
			exitWithError(fmt.Errorf("get accounts: %w", err))
		}
	}

	if cfg.listAccounts {
		printAccounts(accounts)
		return
	}

	if cfg.accountID == "" {
		cfg.accountID, err = resolveAccountID(cfg.accountID, accounts)
		if err != nil {
			exitWithError(err)
		}
	}

	from, to, err := parsePeriod(cfg.fromRaw, cfg.toRaw)
	if err != nil {
		exitWithError(err)
	}

	ops, err := fetchOperations(operationsClient, cfg.appName, cfg.timeout, cfg.accountID, from, to)
	if err != nil {
		exitWithError(fmt.Errorf("fetch operations: %w", err))
	}

	stats, err := buildReport(ops, instrumentsClient, cfg.appName, cfg.timeout)
	if err != nil {
		exitWithError(fmt.Errorf("build report: %w", err))
	}

	printReport(cfg.accountID, from, to, stats, len(ops))
}

func parseFlags() (config, error) {
	cfg := config{}

	flag.StringVar(&cfg.mode, "mode", "report", "execution mode: report|sync|serve|sync-forecasts|evaluate-forecasts|sync-all")
	flag.StringVar(&cfg.token, "token", strings.TrimSpace(os.Getenv("TINVEST_TOKEN")), "T-Invest API token (or env TINVEST_TOKEN)")
	flag.StringVar(&cfg.accountID, "account-id", strings.TrimSpace(os.Getenv("TINVEST_ACCOUNT_ID")), "account id (or env TINVEST_ACCOUNT_ID)")
	flag.StringVar(&cfg.fromRaw, "from", "", "period start, format YYYY-MM-DD or RFC3339 (UTC)")
	flag.StringVar(&cfg.toRaw, "to", "", "period end, format YYYY-MM-DD or RFC3339 (UTC)")
	flag.StringVar(&cfg.endpoint, "endpoint", "", "gRPC endpoint host:port")
	flag.StringVar(&cfg.appName, "app-name", defaultAppName, "x-app-name header")
	flag.StringVar(&cfg.caCertFile, "ca-cert-file", "", "path to extra root CA PEM file for TLS verification")
	flag.StringVar(&cfg.dbPath, "db-path", "./pnl.sqlite", "sqlite database path (sync/serve modes)")
	flag.StringVar(&cfg.httpAddr, "http-addr", ":8080", "http listen address for serve mode")
	flag.BoolVar(&cfg.sandbox, "sandbox", false, "use sandbox endpoint by default")
	flag.BoolVar(&cfg.listAccounts, "list-accounts", false, "print available accounts and exit")
	flag.BoolVar(&cfg.syncOnStart, "sync-on-start", true, "run sync before starting web server (serve mode)")
	flag.BoolVar(&cfg.insecureSkipVerify, "insecure-skip-verify", false, "disable TLS certificate verification (unsafe)")
	flag.BoolVar(&cfg.syncForecasts, "sync-forecasts", false, "sync analyst consensus snapshots")
	flag.BoolVar(&cfg.evaluateForecasts, "evaluate-forecasts", false, "evaluate matured forecast snapshots")
	flag.BoolVar(&cfg.syncAll, "sync-all", false, "run sync + forecast sync + forecast evaluation")
	flag.StringVar(&cfg.forecastProvider, "forecast-provider", strings.TrimSpace(os.Getenv("FORECAST_PROVIDER_PRIMARY")), "forecast provider (default: finnhub)")
	flag.StringVar(&cfg.forecastFallback, "forecast-fallback-provider", strings.TrimSpace(os.Getenv("FORECAST_PROVIDER_FALLBACK")), "optional fallback forecast provider")
	flag.StringVar(&cfg.forecastHorizons, "forecast-horizons", strings.TrimSpace(os.Getenv("FORECAST_HORIZONS")), "forecast evaluation horizons, CSV (e.g. 1M,3M,6M,12M)")
	flag.StringVar(&cfg.finnhubAPIKey, "finnhub-api-key", strings.TrimSpace(os.Getenv("FINNHUB_API_KEY")), "Finnhub API key")
	flag.BoolVar(&cfg.forecastFallbackOn, "forecast-enable-fallback", envBool("FORECAST_ENABLE_FALLBACK", false), "enable fallback provider")
	flag.DurationVar(&cfg.forecastInterval, "forecast-sync-interval", envDuration("FORECAST_SYNC_INTERVAL", 24*time.Hour), "forecast sync interval hint")
	flag.DurationVar(&cfg.timeout, "timeout", 15*time.Second, "per-request timeout")

	flag.Parse()

	cfg.token = strings.TrimSpace(cfg.token)
	cfg.accountID = strings.TrimSpace(cfg.accountID)
	cfg.fromRaw = strings.TrimSpace(cfg.fromRaw)
	cfg.toRaw = strings.TrimSpace(cfg.toRaw)
	cfg.endpoint = strings.TrimSpace(cfg.endpoint)
	cfg.appName = strings.TrimSpace(cfg.appName)
	cfg.caCertFile = strings.TrimSpace(cfg.caCertFile)
	cfg.dbPath = strings.TrimSpace(cfg.dbPath)
	cfg.httpAddr = strings.TrimSpace(cfg.httpAddr)
	cfg.forecastProvider = strings.ToLower(strings.TrimSpace(cfg.forecastProvider))
	cfg.forecastFallback = strings.ToLower(strings.TrimSpace(cfg.forecastFallback))
	cfg.forecastHorizons = strings.TrimSpace(cfg.forecastHorizons)
	cfg.finnhubAPIKey = strings.TrimSpace(cfg.finnhubAPIKey)
	if cfg.appName == "" {
		cfg.appName = defaultAppName
	}
	if cfg.forecastProvider == "" {
		cfg.forecastProvider = "finnhub"
	}
	if cfg.forecastHorizons == "" {
		cfg.forecastHorizons = "1M,3M,6M,12M"
	}
	if cfg.syncAll {
		cfg.mode = "sync-all"
	} else if cfg.syncForecasts {
		cfg.mode = "sync-forecasts"
	} else if cfg.evaluateForecasts {
		cfg.mode = "evaluate-forecasts"
	}
	if cfg.timeout <= 0 {
		return cfg, errors.New("timeout must be > 0")
	}
	if cfg.forecastInterval <= 0 {
		cfg.forecastInterval = 24 * time.Hour
	}

	return cfg, nil
}

func envBool(name string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return v
}

func envDuration(name string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	v, err := time.ParseDuration(raw)
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}

func buildTLSConfig(cfg config) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.insecureSkipVerify,
	}

	if cfg.caCertFile == "" {
		return tlsConfig, nil
	}

	pemData, err := os.ReadFile(cfg.caCertFile)
	if err != nil {
		return nil, fmt.Errorf("read -ca-cert-file: %w", err)
	}

	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(pemData) {
		return nil, errors.New("failed to parse PEM data from -ca-cert-file")
	}
	tlsConfig.RootCAs = pool

	return tlsConfig, nil
}

func getAccounts(client pb.UsersServiceClient, appName string, timeout time.Duration) ([]*pb.Account, error) {
	ctx, cancel := requestContext(appName, timeout)
	defer cancel()

	resp, err := client.GetAccounts(ctx, &pb.GetAccountsRequest{})
	if err != nil {
		return nil, err
	}
	return resp.GetAccounts(), nil
}

func printAccounts(accounts []*pb.Account) {
	if len(accounts) == 0 {
		fmt.Println("No accounts available for this token.")
		return
	}

	fmt.Printf("%-40s %-15s %-10s %-10s %s\n", "ACCOUNT_ID", "TYPE", "STATUS", "ACCESS", "NAME")
	for _, a := range accounts {
		fmt.Printf("%-40s %-15s %-10s %-10s %s\n",
			a.GetId(),
			a.GetType().String(),
			a.GetStatus().String(),
			a.GetAccessLevel().String(),
			a.GetName(),
		)
	}
}

func resolveAccountID(accountID string, accounts []*pb.Account) (string, error) {
	if accountID != "" {
		for _, a := range accounts {
			if a.GetId() == accountID {
				return accountID, nil
			}
		}
		return "", fmt.Errorf("account-id %q is not available for this token", accountID)
	}

	open := make([]*pb.Account, 0, len(accounts))
	for _, a := range accounts {
		if a.GetStatus() == pb.AccountStatus_ACCOUNT_STATUS_OPEN {
			open = append(open, a)
		}
	}

	if len(open) == 1 {
		return open[0].GetId(), nil
	}

	if len(open) == 0 {
		return "", errors.New("no open accounts available for this token")
	}

	return "", errors.New("multiple open accounts found; pass -account-id or run with -list-accounts")
}

func parsePeriod(fromRaw, toRaw string) (time.Time, time.Time, error) {
	now := time.Now().UTC()
	startOfMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	from := startOfMonth
	to := now
	var err error

	if fromRaw != "" {
		from, err = parseDateOrTimestamp(fromRaw, false)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid -from: %w", err)
		}
	}

	if toRaw != "" {
		to, err = parseDateOrTimestamp(toRaw, true)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid -to: %w", err)
		}
	}

	if !from.Before(to) {
		return time.Time{}, time.Time{}, errors.New("period must satisfy from < to")
	}

	return from, to, nil
}

func parseDateOrTimestamp(raw string, endOfDay bool) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.UTC(), nil
	}

	t, err := time.ParseInLocation("2006-01-02", raw, time.UTC)
	if err != nil {
		return time.Time{}, fmt.Errorf("expected YYYY-MM-DD or RFC3339, got %q", raw)
	}
	if endOfDay {
		return t.Add(24*time.Hour - time.Nanosecond), nil
	}
	return t, nil
}

func fetchOperations(client pb.OperationsServiceClient, appName string, timeout time.Duration, accountID string, from, to time.Time) ([]*pb.OperationItem, error) {
	var (
		cursor string
		items  []*pb.OperationItem
	)

	for {
		ctx, cancel := requestContext(appName, timeout)
		resp, err := client.GetOperationsByCursor(ctx, &pb.GetOperationsByCursorRequest{
			AccountId:          accountID,
			From:               timestamppb.New(from),
			To:                 timestamppb.New(to),
			Cursor:             cursor,
			Limit:              1000,
			State:              pb.OperationState_OPERATION_STATE_EXECUTED,
			WithoutCommissions: false,
			WithoutTrades:      true,
			WithoutOvernights:  true,
		})
		cancel()
		if err != nil {
			return nil, err
		}

		items = append(items, resp.GetItems()...)
		if !resp.GetHasNext() {
			break
		}

		next := strings.TrimSpace(resp.GetNextCursor())
		if next == "" || next == cursor {
			break
		}
		cursor = next
	}

	sort.Slice(items, func(i, j int) bool {
		ti := items[i].GetDate().AsTime()
		tj := items[j].GetDate().AsTime()
		if ti.Equal(tj) {
			return items[i].GetId() < items[j].GetId()
		}
		return ti.Before(tj)
	})

	return items, nil
}

func buildReport(ops []*pb.OperationItem, instrumentsClient pb.InstrumentsServiceClient, appName string, timeout time.Duration) ([]instrumentStat, error) {
	positions := make(map[string]*positionState)
	stats := make(map[string]*instrumentStat)

	tickerCache := make(map[string]struct {
		ticker string
		name   string
	})

	for _, op := range ops {
		if !isShare(op) {
			continue
		}

		qty := op.GetQuantityDone()
		if qty <= 0 {
			qty = op.GetQuantity()
		}

		figi := strings.TrimSpace(op.GetFigi())
		if figi == "" {
			// Для части инструментов FIGI может быть пустым, fallback на instrument_uid.
			figi = strings.TrimSpace(op.GetInstrumentUid())
		}
		if figi == "" {
			continue
		}

		st, ok := stats[figi]
		if !ok {
			st = &instrumentStat{figi: figi}
			stats[figi] = st
		}

		if st.currency == "" {
			st.currency = strings.ToUpper(strings.TrimSpace(op.GetPayment().GetCurrency()))
		}

		if st.ticker == "" {
			meta, ok := tickerCache[figi]
			if !ok {
				ticker, name, err := getInstrumentMeta(instrumentsClient, appName, timeout, figi)
				if err != nil {
					// Метаданные не критичны для отчета, продолжаем с FIGI.
					ticker = figi
				}
				meta = struct {
					ticker string
					name   string
				}{ticker: ticker, name: name}
				tickerCache[figi] = meta
			}
			st.ticker = meta.ticker
			st.name = meta.name
		}

		pay := math.Abs(moneyValueToFloat(op.GetPayment()))
		fee := math.Abs(moneyValueToFloat(op.GetCommission()))

		switch op.GetType() {
		case pb.OperationType_OPERATION_TYPE_BUY,
			pb.OperationType_OPERATION_TYPE_BUY_CARD,
			pb.OperationType_OPERATION_TYPE_BUY_MARGIN,
			pb.OperationType_OPERATION_TYPE_DELIVERY_BUY:
			if qty <= 0 {
				continue
			}
			total := pay + fee
			if total == 0 {
				total = float64(qty) * math.Abs(moneyValueToFloat(op.GetPrice()))
			}
			if total <= 0 {
				continue
			}
			pushLot(positions, figi, qty, total/float64(qty))
			st.boughtQty += qty
			st.buyAmount += total
		case pb.OperationType_OPERATION_TYPE_SELL,
			pb.OperationType_OPERATION_TYPE_SELL_CARD,
			pb.OperationType_OPERATION_TYPE_SELL_MARGIN,
			pb.OperationType_OPERATION_TYPE_DELIVERY_SELL:
			if qty <= 0 {
				continue
			}
			proceeds := pay - fee
			if proceeds == 0 {
				proceeds = float64(qty) * math.Abs(moneyValueToFloat(op.GetPrice()))
			}
			if proceeds <= 0 {
				continue
			}

			cost := popLotsCost(positions, figi, qty)
			st.soldQty += qty
			st.sellAmount += proceeds
			st.realizedPnL += proceeds - cost
		case pb.OperationType_OPERATION_TYPE_DIVIDEND,
			pb.OperationType_OPERATION_TYPE_DIV_EXT,
			pb.OperationType_OPERATION_TYPE_DIVIDEND_TRANSFER:
			st.dividendPnL += moneyValueToFloat(op.GetPayment())
		}
	}

	rows := make([]instrumentStat, 0, len(stats))
	for figi, st := range stats {
		if st.ticker == "" {
			st.ticker = figi
		}

		if ps, ok := positions[figi]; ok {
			for _, l := range ps.lots {
				st.openQty += l.qty
				st.openCost += float64(l.qty) * l.unitCost
			}
		}

		rows = append(rows, *st)
	}

	sort.Slice(rows, func(i, j int) bool {
		totalI := rows[i].realizedPnL + rows[i].dividendPnL
		totalJ := rows[j].realizedPnL + rows[j].dividendPnL
		if totalI == totalJ {
			return rows[i].ticker < rows[j].ticker
		}
		return totalI > totalJ
	})

	return rows, nil
}

func isShare(op *pb.OperationItem) bool {
	if op.GetInstrumentKind() == pb.InstrumentType_INSTRUMENT_TYPE_SHARE {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(op.GetInstrumentType()), "share")
}

func pushLot(positions map[string]*positionState, key string, qty int64, unitCost float64) {
	ps, ok := positions[key]
	if !ok {
		ps = &positionState{}
		positions[key] = ps
	}
	ps.lots = append(ps.lots, lot{qty: qty, unitCost: unitCost})
}

func popLotsCost(positions map[string]*positionState, key string, qty int64) float64 {
	ps, ok := positions[key]
	if !ok || qty <= 0 {
		return 0
	}

	remaining := qty
	var total float64

	for remaining > 0 && len(ps.lots) > 0 {
		head := &ps.lots[0]
		take := remaining
		if head.qty < take {
			take = head.qty
		}
		total += float64(take) * head.unitCost
		head.qty -= take
		remaining -= take

		if head.qty == 0 {
			ps.lots = ps.lots[1:]
		}
	}

	return total
}

func getInstrumentMeta(client pb.InstrumentsServiceClient, appName string, timeout time.Duration, figi string) (ticker string, name string, err error) {
	ctx, cancel := requestContext(appName, timeout)
	defer cancel()

	resp, err := client.GetInstrumentBy(ctx, &pb.InstrumentRequest{
		IdType: pb.InstrumentIdType_INSTRUMENT_ID_TYPE_FIGI,
		Id:     figi,
	})
	if err != nil {
		return "", "", err
	}
	if resp.GetInstrument() == nil {
		return figi, "", nil
	}
	return resp.GetInstrument().GetTicker(), resp.GetInstrument().GetName(), nil
}

func moneyValueToFloat(v *pb.MoneyValue) float64 {
	if v == nil {
		return 0
	}
	return float64(v.GetUnits()) + float64(v.GetNano())/1e9
}

func requestContext(appName string, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	ctx = metadata.AppendToOutgoingContext(ctx, "x-app-name", appName)
	return ctx, cancel
}

func printReport(accountID string, from, to time.Time, rows []instrumentStat, totalOps int) {
	fmt.Printf("Account: %s\n", accountID)
	fmt.Printf("Period (UTC): %s .. %s\n", from.Format(time.RFC3339), to.Format(time.RFC3339))
	fmt.Printf("Processed operations: %d\n\n", totalOps)

	if len(rows) == 0 {
		fmt.Println("No stock buy/sell operations found for the selected period.")
		return
	}

	fmt.Printf("%-10s %-16s %-6s %10s %10s %10s %14s %14s %14s %14s %14s\n",
		"TICKER", "FIGI/UID", "CUR", "BUY_QTY", "SELL_QTY", "OPEN_QTY", "BUY_SUM", "SELL_SUM", "REALIZED", "DIVIDENDS", "TOTAL")

	var totalBuy, totalSell, totalRealized, totalDividends float64
	for _, r := range rows {
		totalPnL := r.realizedPnL + r.dividendPnL
		fmt.Printf("%-10s %-16s %-6s %10d %10d %10d %14.2f %14.2f %14.2f %14.2f %14.2f\n",
			trimLen(r.ticker, 10),
			trimLen(r.figi, 16),
			r.currency,
			r.boughtQty,
			r.soldQty,
			r.openQty,
			r.buyAmount,
			r.sellAmount,
			r.realizedPnL,
			r.dividendPnL,
			totalPnL,
		)
		totalBuy += r.buyAmount
		totalSell += r.sellAmount
		totalRealized += r.realizedPnL
		totalDividends += r.dividendPnL
	}

	fmt.Println(strings.Repeat("-", 144))
	fmt.Printf("%-10s %-16s %-6s %10s %10s %10s %14.2f %14.2f %14.2f %14.2f %14.2f\n",
		"TOTAL", "", "", "", "", "", totalBuy, totalSell, totalRealized, totalDividends, totalRealized+totalDividends)
}

func trimLen(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 1 {
		return s[:n]
	}
	return s[:n-1] + "~"
}

func exitWithError(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}
