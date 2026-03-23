package main

import "testing"

func TestRecommendationScore(t *testing.T) {
	counts := providerRecommendationCounts{
		StrongBuy:  2,
		Buy:        3,
		Hold:       1,
		Sell:       1,
		StrongSell: 1,
	}
	score, ok := counts.ratingScore()
	if !ok {
		t.Fatalf("expected score to be available")
	}
	// (2*2 + 3 - 1 - 2*1) / 8 = 0.5
	if score != 0.5 {
		t.Fatalf("unexpected score %.6f", score)
	}
}

func TestParseForecastHorizons(t *testing.T) {
	h, err := parseForecastHorizons("12M,1M,3M,1M")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(h) != 3 {
		t.Fatalf("expected 3 horizons, got %d", len(h))
	}
	if h[0].Code != "1M" || h[1].Code != "3M" || h[2].Code != "12M" {
		t.Fatalf("unexpected horizon order: %+v", h)
	}
}

func TestAggregateConsensusTarget(t *testing.T) {
	rows := []providerConsensusData{
		{TargetAvailable: true, TargetPrice: 100, TargetCurrency: "USD"},
		{TargetAvailable: true, TargetPrice: 200, TargetCurrency: "USD"},
		{TargetAvailable: false},
	}
	avg, cur, ok := aggregateConsensusTarget(rows, "USD")
	if !ok {
		t.Fatalf("expected aggregate target")
	}
	if avg != 150 {
		t.Fatalf("unexpected avg %.4f", avg)
	}
	if cur != "USD" {
		t.Fatalf("unexpected currency %q", cur)
	}
}

func TestSignFloat(t *testing.T) {
	if signFloat(0) != 0 {
		t.Fatalf("zero should map to 0")
	}
	if signFloat(0.1) != 1 {
		t.Fatalf("positive should map to 1")
	}
	if signFloat(-0.1) != -1 {
		t.Fatalf("negative should map to -1")
	}
}
