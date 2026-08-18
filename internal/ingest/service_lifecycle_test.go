package ingest_test

import (
	"context"
	"testing"

	"github.com/convin/webhook-ingest/internal/config"
	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/redisclient"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/testutil"
)

func TestWaitWaitsForInFlightRecording(t *testing.T) {
	st := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, st)

	cfg := config.Load()

	rdb, err := redisclient.New(context.Background(), cfg.RedisAddr)
	if err != nil {
		t.Fatalf("connect to redis: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	svc := ingest.New(
		st,
		stats.NewCache(),
		rdb,
		nil,
	)

	evt := ingest.Event{
		EventID:      eventID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  10,
		RecordingURL: "https://example.com/test.wav",
	}

	if err := svc.Ingest(context.Background(), evt); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// Wait must not return until the in-flight recording is complete.
	svc.Wait()

	var processed bool
	err = st.Pool().QueryRow(
		context.Background(),
		`SELECT recording_processed
		 FROM calls
		 WHERE call_id = $1`,
		callID,
	).Scan(&processed)
	if err != nil {
		t.Fatalf("query recording status: %v", err)
	}

	if !processed {
		t.Fatal("recording was not processed before Wait returned")
	}
}
