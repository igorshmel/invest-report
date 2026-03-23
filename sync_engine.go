package main

import (
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	pb "github.com/tinkoff/invest-api-go-sdk/proto"
	"google.golang.org/protobuf/encoding/protojson"
)

type apiClients struct {
	operations  pb.OperationsServiceClient
	instruments pb.InstrumentsServiceClient
	marketData  pb.MarketDataServiceClient
}

type rawCalcOperation struct {
	OperationID    string
	InstrumentID   string
	Side           string
	Quantity       int64
	PaymentNano    int64
	CommissionNano int64
	PriceNano      int64
	Currency       string
	ExecutedAt     time.Time
}

type openBuyLot struct {
	Qty         int64
	CostNano    int64
	OperationID string
	OpenTime    time.Time
	Currency    string
}

type instrumentCalcState struct {
	InstrumentID string
	Currency     string
	Lots         []openBuyLot
	PendingSell  int64
	Seen         bool
}

type splitAdjustment struct {
	effectiveAt time.Time
	num         int64
	den         int64
}

var accountSyncGuard = struct {
	mu      sync.Mutex
	running map[string]bool
}{
	running: map[string]bool{},
}

func syncAndRebuild(db *sql.DB, api *apiClients, cfg config, accountID string) error {
	release, err := acquireSyncLock(accountID)
	if err != nil {
		return err
	}
	defer release()

	if err := setSyncState(db, accountID, "RUNNING", "", "", ""); err != nil {
		return fmt.Errorf("set RUNNING sync state: %w", err)
	}

	lastCursor, err := syncOperationsToDB(db, api, cfg, accountID)
	if err != nil {
		_ = setSyncState(db, accountID, "FAILED", lastCursor, "", err.Error())
		return fmt.Errorf("sync operations: %w", err)
	}

	inconsistencies, err := rebuildClosedDeals(db, accountID)
	if err != nil {
		_ = setSyncState(db, accountID, "FAILED", lastCursor, "", err.Error())
		return fmt.Errorf("rebuild deals: %w", err)
	}

	errMsg := ""
	if inconsistencies > 0 {
		errMsg = fmt.Sprintf("detected %d inconsistent sell allocations (sell quantity exceeded open lots)", inconsistencies)
	}
	if err := setSyncState(db, accountID, "IDLE", lastCursor, time.Now().UTC().Format(time.RFC3339Nano), errMsg); err != nil {
		return fmt.Errorf("set IDLE sync state: %w", err)
	}

	return nil
}

func acquireSyncLock(accountID string) (func(), error) {
	accountSyncGuard.mu.Lock()
	if accountSyncGuard.running[accountID] {
		accountSyncGuard.mu.Unlock()
		return nil, fmt.Errorf("sync is already running for account %s", accountID)
	}
	accountSyncGuard.running[accountID] = true
	accountSyncGuard.mu.Unlock()

	return func() {
		accountSyncGuard.mu.Lock()
		delete(accountSyncGuard.running, accountID)
		accountSyncGuard.mu.Unlock()
	}, nil
}

func setSyncState(db *sql.DB, accountID, status, lastCursor, lastSyncedAt, errMsg string) error {
	_, err := db.Exec(`
		INSERT INTO sync_state (account_id, last_cursor, last_synced_at, status, error_message)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(account_id) DO UPDATE SET
			last_cursor = excluded.last_cursor,
			last_synced_at = excluded.last_synced_at,
			status = excluded.status,
			error_message = excluded.error_message
	`, accountID, lastCursor, lastSyncedAt, status, errMsg)
	return err
}

func syncOperationsToDB(db *sql.DB, api *apiClients, cfg config, accountID string) (string, error) {
	cursor := ""

	for {
		ctx, cancel := requestContext(cfg.appName, cfg.timeout)
		resp, err := api.operations.GetOperationsByCursor(ctx, &pb.GetOperationsByCursorRequest{
			AccountId:          accountID,
			Cursor:             cursor,
			Limit:              1000,
			State:              pb.OperationState_OPERATION_STATE_EXECUTED,
			WithoutCommissions: false,
			WithoutTrades:      true,
			WithoutOvernights:  true,
		})
		cancel()
		if err != nil {
			return cursor, err
		}

		tx, err := db.Begin()
		if err != nil {
			return cursor, err
		}

		now := time.Now().UTC().Format(time.RFC3339Nano)
		for _, item := range resp.GetItems() {
			if err := upsertOperationTx(tx, accountID, item, now); err != nil {
				_ = tx.Rollback()
				return cursor, err
			}
		}

		if err := tx.Commit(); err != nil {
			return cursor, err
		}

		if !resp.GetHasNext() {
			break
		}
		next := strings.TrimSpace(resp.GetNextCursor())
		if next == "" || next == cursor {
			break
		}
		cursor = next
	}

	if err := enrichInstrumentMetadata(db, api, cfg, accountID); err != nil {
		return cursor, err
	}

	return cursor, nil
}

func upsertOperationTx(tx *sql.Tx, accountID string, item *pb.OperationItem, now string) error {
	operationID := strings.TrimSpace(item.GetId())
	if operationID == "" {
		return nil
	}

	figi := strings.TrimSpace(item.GetFigi())
	instrumentUID := strings.TrimSpace(item.GetInstrumentUid())
	instrumentID := figi
	if instrumentID == "" {
		instrumentID = instrumentUID
	}

	if instrumentID != "" {
		if _, err := tx.Exec(`
			INSERT INTO instruments (instrument_id, figi, instrument_uid, ticker, name, currency, updated_at)
			VALUES (?, ?, ?, '', '', ?, ?)
			ON CONFLICT(instrument_id) DO UPDATE SET
				figi = COALESCE(NULLIF(excluded.figi, ''), instruments.figi),
				instrument_uid = COALESCE(NULLIF(excluded.instrument_uid, ''), instruments.instrument_uid),
				currency = COALESCE(NULLIF(excluded.currency, ''), instruments.currency),
				updated_at = excluded.updated_at
		`, instrumentID, figi, instrumentUID, strings.ToUpper(strings.TrimSpace(item.GetPayment().GetCurrency())), now); err != nil {
			return err
		}
	}

	payload, err := protojson.Marshal(item)
	if err != nil {
		payload = []byte("{}")
	}

	executedAt := item.GetDate().AsTime().UTC()
	if executedAt.IsZero() {
		executedAt = time.Now().UTC()
	}

	qty := item.GetQuantityDone()
	if qty <= 0 {
		qty = item.GetQuantity()
	}

	currency := strings.ToUpper(strings.TrimSpace(item.GetPayment().GetCurrency()))
	if currency == "" {
		currency = strings.ToUpper(strings.TrimSpace(item.GetPrice().GetCurrency()))
	}

	_, err = tx.Exec(`
		INSERT INTO operations_raw (
			operation_id, account_id, instrument_id, figi, instrument_uid,
			side, operation_type, quantity, payment_nano, commission_nano, price_nano,
			currency, executed_at, payload_json, created_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(operation_id) DO UPDATE SET
			account_id = excluded.account_id,
			instrument_id = excluded.instrument_id,
			figi = excluded.figi,
			instrument_uid = excluded.instrument_uid,
			side = excluded.side,
			operation_type = excluded.operation_type,
			quantity = excluded.quantity,
			payment_nano = excluded.payment_nano,
			commission_nano = excluded.commission_nano,
			price_nano = excluded.price_nano,
			currency = excluded.currency,
			executed_at = excluded.executed_at,
			payload_json = excluded.payload_json,
			created_at = excluded.created_at
	`,
		operationID,
		accountID,
		instrumentID,
		figi,
		instrumentUID,
		operationSide(item.GetType()),
		item.GetType().String(),
		qty,
		moneyValueToNano(item.GetPayment()),
		moneyValueToNano(item.GetCommission()),
		moneyValueToNano(item.GetPrice()),
		currency,
		executedAt.Format(time.RFC3339Nano),
		string(payload),
		now,
	)
	return err
}

func enrichInstrumentMetadata(db *sql.DB, api *apiClients, cfg config, accountID string) error {
	rows, err := db.Query(`
		SELECT DISTINCT i.instrument_id, COALESCE(i.figi, ''), COALESCE(i.instrument_uid, '')
		FROM instruments i
		JOIN operations_raw o ON o.instrument_id = i.instrument_id
		WHERE o.account_id = ?
			AND (COALESCE(i.ticker, '') = '' OR COALESCE(i.name, '') = '')
	`, accountID)
	if err != nil {
		return err
	}
	defer rows.Close()

	type rowData struct {
		instrumentID string
		figi         string
		uid          string
	}
	pending := make([]rowData, 0)
	for rows.Next() {
		var r rowData
		if err := rows.Scan(&r.instrumentID, &r.figi, &r.uid); err != nil {
			return err
		}
		pending = append(pending, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, p := range pending {
		idType := pb.InstrumentIdType_INSTRUMENT_ID_UNSPECIFIED
		id := ""
		switch {
		case p.figi != "":
			idType = pb.InstrumentIdType_INSTRUMENT_ID_TYPE_FIGI
			id = p.figi
		case p.uid != "":
			idType = pb.InstrumentIdType_INSTRUMENT_ID_TYPE_UID
			id = p.uid
		}
		if idType == pb.InstrumentIdType_INSTRUMENT_ID_UNSPECIFIED || id == "" {
			continue
		}

		ctx, cancel := requestContext(cfg.appName, cfg.timeout)
		resp, err := api.instruments.GetInstrumentBy(ctx, &pb.InstrumentRequest{IdType: idType, Id: id})
		cancel()
		if err != nil || resp.GetInstrument() == nil {
			continue
		}

		inst := resp.GetInstrument()
		_, _ = db.Exec(`
			UPDATE instruments
			SET ticker = ?, name = ?, currency = COALESCE(NULLIF(?, ''), currency), updated_at = ?
			WHERE instrument_id = ?
		`, strings.TrimSpace(inst.GetTicker()), strings.TrimSpace(inst.GetName()), strings.ToUpper(strings.TrimSpace(inst.GetCurrency())), now, p.instrumentID)
	}

	return nil
}

func rebuildClosedDeals(db *sql.DB, accountID string) (int, error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.Exec(`DELETE FROM deal_operation_links WHERE deal_id IN (SELECT deal_id FROM deals_closed WHERE account_id = ?)`, accountID); err != nil {
		return 0, err
	}
	if _, err = tx.Exec(`DELETE FROM deals_closed WHERE account_id = ?`, accountID); err != nil {
		return 0, err
	}
	if _, err = tx.Exec(`DELETE FROM open_deals WHERE account_id = ?`, accountID); err != nil {
		return 0, err
	}
	if _, err = tx.Exec(`DELETE FROM instrument_positions WHERE account_id = ?`, accountID); err != nil {
		return 0, err
	}

	splitMap, err := loadSplitAdjustmentsTx(tx)
	if err != nil {
		return 0, err
	}

	rows, err := tx.Query(`
		SELECT operation_id, instrument_id, side, quantity, payment_nano, commission_nano, price_nano, currency, executed_at
		FROM operations_raw
		WHERE account_id = ?
			AND instrument_id IS NOT NULL
			AND instrument_id <> ''
			AND side IN ('BUY', 'SELL')
			AND quantity > 0
		ORDER BY instrument_id, executed_at, operation_id
	`, accountID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	insertDealStmt, err := tx.Prepare(`
		INSERT INTO deals_closed (
			deal_id, account_id, instrument_id,
			buy_qty, buy_sum_nano, sell_qty, sell_sum_nano, final_pnl_nano,
			currency, open_time, close_time, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return 0, err
	}
	defer insertDealStmt.Close()

	insertLinkStmt, err := tx.Prepare(`
		INSERT INTO deal_operation_links (
			deal_id, operation_id, role, allocated_qty, allocated_nano
		) VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return 0, err
	}
	defer insertLinkStmt.Close()

	insertPositionStmt, err := tx.Prepare(`
		INSERT INTO instrument_positions (
			account_id, instrument_id, open_qty, open_cost_nano, status, updated_at
		) VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return 0, err
	}
	defer insertPositionStmt.Close()

	insertOpenDealStmt, err := tx.Prepare(`
		INSERT INTO open_deals (
			open_deal_id, account_id, instrument_id, buy_operation_id,
			open_time, open_qty, open_cost_nano, currency, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return 0, err
	}
	defer insertOpenDealStmt.Close()

	var (
		currentInstrument string
		state             instrumentCalcState
		inconsistencies   int
		currentSplits     []splitAdjustment
		splitIdx          int
	)

	applyPendingSplits := func(until time.Time) {
		if len(currentSplits) == 0 {
			return
		}
		for splitIdx < len(currentSplits) && !currentSplits[splitIdx].effectiveAt.After(until) {
			applySplitToState(&state, currentSplits[splitIdx])
			splitIdx++
		}
	}
	applyAllRemainingSplits := func() {
		for splitIdx < len(currentSplits) {
			applySplitToState(&state, currentSplits[splitIdx])
			splitIdx++
		}
	}

	flushState := func(now string) error {
		if !state.Seen || state.InstrumentID == "" {
			return nil
		}
		var openQty, openCost int64
		for idx, l := range state.Lots {
			openQty += l.Qty
			openCost += l.CostNano

			currency := l.Currency
			if currency == "" {
				currency = state.Currency
			}
			openDealID := makeOpenDealID(accountID, state.InstrumentID, l.OperationID, l.OpenTime, idx)
			if _, err := insertOpenDealStmt.Exec(
				openDealID,
				accountID,
				state.InstrumentID,
				l.OperationID,
				l.OpenTime.UTC().Format(time.RFC3339Nano),
				l.Qty,
				l.CostNano,
				currency,
				now,
			); err != nil {
				return err
			}
		}
		status := "INACTIVE"
		if openQty > 0 {
			status = "ACTIVE"
		}
		_, err := insertPositionStmt.Exec(accountID, state.InstrumentID, openQty, openCost, status, now)
		if err == nil && state.PendingSell > 0 {
			inconsistencies++
		}
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for rows.Next() {
		var (
			opID, instrumentID, side, currency, executedRaw string
			qty, paymentNano, commissionNano, priceNano     int64
		)
		if err := rows.Scan(&opID, &instrumentID, &side, &qty, &paymentNano, &commissionNano, &priceNano, &currency, &executedRaw); err != nil {
			return 0, err
		}
		executedAt, err := time.Parse(time.RFC3339Nano, executedRaw)
		if err != nil {
			return 0, fmt.Errorf("parse executed_at for operation %s: %w", opID, err)
		}

		op := rawCalcOperation{
			OperationID:    opID,
			InstrumentID:   instrumentID,
			Side:           side,
			Quantity:       qty,
			PaymentNano:    paymentNano,
			CommissionNano: commissionNano,
			PriceNano:      priceNano,
			Currency:       strings.ToUpper(strings.TrimSpace(currency)),
			ExecutedAt:     executedAt,
		}

		if currentInstrument == "" {
			currentInstrument = op.InstrumentID
			state = instrumentCalcState{InstrumentID: op.InstrumentID, Currency: op.Currency, Seen: true}
			currentSplits = splitMap[currentInstrument]
			splitIdx = 0
		}

		if op.InstrumentID != currentInstrument {
			applyAllRemainingSplits()
			if err := flushState(now); err != nil {
				return 0, err
			}
			currentInstrument = op.InstrumentID
			state = instrumentCalcState{InstrumentID: op.InstrumentID, Currency: op.Currency, Seen: true}
			currentSplits = splitMap[currentInstrument]
			splitIdx = 0
		}

		if state.Currency == "" && op.Currency != "" {
			state.Currency = op.Currency
		}
		applyPendingSplits(op.ExecutedAt)

		switch op.Side {
		case "BUY":
			totalBuy := calculateBuySum(op)
			if totalBuy <= 0 {
				continue
			}
			buyQty := op.Quantity
			if buyQty <= 0 {
				continue
			}

			remainingBuyCost := totalBuy
			if state.PendingSell > 0 {
				coverQty := minInt64(buyQty, state.PendingSell)
				_ = allocateFromRemainder(&remainingBuyCost, buyQty, coverQty)
				buyQty -= coverQty
				state.PendingSell -= coverQty
			}
			if buyQty <= 0 {
				continue
			}

			state.Lots = append(state.Lots, openBuyLot{
				Qty:         buyQty,
				CostNano:    remainingBuyCost,
				OperationID: op.OperationID,
				OpenTime:    op.ExecutedAt,
				Currency:    op.Currency,
			})
		case "SELL":
			totalSell := calculateSellSum(op)
			if totalSell <= 0 {
				continue
			}

			remainingQty := op.Quantity
			remainingSell := totalSell
			partIdx := 0

			for remainingQty > 0 && len(state.Lots) > 0 {
				lot := &state.Lots[0]
				takeQty := minInt64(remainingQty, lot.Qty)
				buyAllocated := allocateFromLot(lot, takeQty)
				sellAllocated := allocateFromRemainder(&remainingSell, remainingQty, takeQty)
				openTime := lot.OpenTime
				buyOperationID := lot.OperationID

				if lot.Qty == 0 {
					state.Lots = state.Lots[1:]
				}

				remainingQty -= takeQty

				currency := op.Currency
				if currency == "" {
					currency = state.Currency
				}

				dealID := makeDealID(accountID, op.InstrumentID, op.OperationID, buyOperationID, partIdx)
				if _, err := insertDealStmt.Exec(
					dealID,
					accountID,
					op.InstrumentID,
					takeQty,
					buyAllocated,
					takeQty,
					sellAllocated,
					sellAllocated-buyAllocated,
					currency,
					openTime.UTC().Format(time.RFC3339Nano),
					op.ExecutedAt.UTC().Format(time.RFC3339Nano),
					now,
				); err != nil {
					return 0, err
				}

				if _, err := insertLinkStmt.Exec(dealID, buyOperationID, "BUY_PART", takeQty, buyAllocated); err != nil {
					return 0, err
				}
				if _, err := insertLinkStmt.Exec(dealID, op.OperationID, "SELL_PART", takeQty, sellAllocated); err != nil {
					return 0, err
				}

				partIdx++
			}

			if remainingQty > 0 {
				state.PendingSell += remainingQty
			}
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	applyAllRemainingSplits()
	if err := flushState(now); err != nil {
		return 0, err
	}

	if _, err := tx.Exec(`
		INSERT INTO instrument_positions (
			account_id, instrument_id, open_qty, open_cost_nano, status, updated_at
		)
		SELECT
			?,
			o.instrument_id,
			0,
			0,
			'INACTIVE',
			?
		FROM operations_raw o
		WHERE o.account_id = ?
			AND o.instrument_id IS NOT NULL
			AND o.instrument_id <> ''
			AND o.operation_type IN (
				'OPERATION_TYPE_DIVIDEND',
				'OPERATION_TYPE_DIV_EXT',
				'OPERATION_TYPE_DIVIDEND_TRANSFER'
			)
			AND NOT EXISTS (
				SELECT 1
				FROM instrument_positions p
				WHERE p.account_id = ?
					AND p.instrument_id = o.instrument_id
			)
		GROUP BY o.instrument_id
	`, accountID, now, accountID, accountID); err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return inconsistencies, nil
}

func loadSplitAdjustmentsTx(tx *sql.Tx) (map[string][]splitAdjustment, error) {
	rows, err := tx.Query(`
		SELECT instrument_id, effective_at, ratio_num, ratio_den
		FROM corporate_actions
		WHERE action_type = 'SPLIT'
			AND ratio_num > 0
			AND ratio_den > 0
		ORDER BY instrument_id, effective_at
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string][]splitAdjustment)
	for rows.Next() {
		var instrumentID, effectiveAtRaw string
		var num, den int64
		if err := rows.Scan(&instrumentID, &effectiveAtRaw, &num, &den); err != nil {
			return nil, err
		}
		effectiveAt, err := time.Parse(time.RFC3339Nano, effectiveAtRaw)
		if err != nil {
			return nil, fmt.Errorf("parse split effective_at for instrument %s: %w", instrumentID, err)
		}
		out[instrumentID] = append(out[instrumentID], splitAdjustment{
			effectiveAt: effectiveAt,
			num:         num,
			den:         den,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func applySplitToState(state *instrumentCalcState, split splitAdjustment) {
	if state == nil || split.num <= 0 || split.den <= 0 || len(state.Lots) == 0 {
		if state != nil && state.PendingSell > 0 {
			state.PendingSell = mulDivInt64(state.PendingSell, split.num, split.den)
		}
		return
	}

	newLots := make([]openBuyLot, 0, len(state.Lots))
	for _, lot := range state.Lots {
		if lot.Qty <= 0 {
			continue
		}
		newQty := mulDivInt64(lot.Qty, split.num, split.den)
		if newQty <= 0 {
			continue
		}
		lot.Qty = newQty
		newLots = append(newLots, lot)
	}
	state.Lots = newLots
	if state.PendingSell > 0 {
		state.PendingSell = mulDivInt64(state.PendingSell, split.num, split.den)
	}
}

func operationSide(opType pb.OperationType) string {
	switch opType {
	case pb.OperationType_OPERATION_TYPE_BUY,
		pb.OperationType_OPERATION_TYPE_BUY_CARD,
		pb.OperationType_OPERATION_TYPE_BUY_MARGIN,
		pb.OperationType_OPERATION_TYPE_DELIVERY_BUY:
		return "BUY"
	case pb.OperationType_OPERATION_TYPE_SELL,
		pb.OperationType_OPERATION_TYPE_SELL_CARD,
		pb.OperationType_OPERATION_TYPE_SELL_MARGIN,
		pb.OperationType_OPERATION_TYPE_DELIVERY_SELL:
		return "SELL"
	default:
		return "OTHER"
	}
}

func moneyValueToNano(v *pb.MoneyValue) int64 {
	if v == nil {
		return 0
	}
	return v.GetUnits()*1_000_000_000 + int64(v.GetNano())
}

func calculateBuySum(op rawCalcOperation) int64 {
	gross := absInt64(op.PaymentNano)
	fees := absInt64(op.CommissionNano)
	total := gross + fees
	if total == 0 && op.PriceNano != 0 && op.Quantity > 0 {
		total = mulAbs(op.PriceNano, op.Quantity)
	}
	return total
}

func calculateSellSum(op rawCalcOperation) int64 {
	gross := absInt64(op.PaymentNano)
	if gross == 0 && op.PriceNano != 0 && op.Quantity > 0 {
		gross = mulAbs(op.PriceNano, op.Quantity)
	}
	net := gross - absInt64(op.CommissionNano)
	if net < 0 {
		return 0
	}
	return net
}

func allocateFromLot(lot *openBuyLot, qty int64) int64 {
	if qty <= 0 || lot.Qty <= 0 {
		return 0
	}
	if qty >= lot.Qty {
		allocated := lot.CostNano
		lot.Qty = 0
		lot.CostNano = 0
		return allocated
	}
	allocated := mulDivInt64(lot.CostNano, qty, lot.Qty)
	lot.Qty -= qty
	lot.CostNano -= allocated
	return allocated
}

func allocateFromRemainder(remaining *int64, remainingQty, qty int64) int64 {
	if remaining == nil || remainingQty <= 0 || qty <= 0 {
		return 0
	}
	if qty >= remainingQty {
		allocated := *remaining
		*remaining = 0
		return allocated
	}
	allocated := mulDivInt64(*remaining, qty, remainingQty)
	*remaining -= allocated
	return allocated
}

func makeDealID(accountID, instrumentID, sellOpID, buyOpID string, partIdx int) string {
	raw := fmt.Sprintf("%s|%s|%s|%s|%d", accountID, instrumentID, sellOpID, buyOpID, partIdx)
	sum := sha1.Sum([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func makeOpenDealID(accountID, instrumentID, buyOpID string, openTime time.Time, idx int) string {
	raw := fmt.Sprintf("%s|%s|%s|%s|%d", accountID, instrumentID, buyOpID, openTime.UTC().Format(time.RFC3339Nano), idx)
	sum := sha1.Sum([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func mulAbs(a, b int64) int64 {
	if a == 0 || b == 0 {
		return 0
	}
	x := big.NewInt(absInt64(a))
	x.Mul(x, big.NewInt(absInt64(b)))
	if !x.IsInt64() {
		return 0
	}
	return x.Int64()
}

func mulDivInt64(a, b, c int64) int64 {
	if c == 0 {
		return 0
	}
	x := big.NewInt(a)
	x.Mul(x, big.NewInt(b))
	x.Div(x, big.NewInt(c))
	if !x.IsInt64() {
		return 0
	}
	return x.Int64()
}

func absInt64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
