package ingest_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

func TestConcurrentDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	const deliveries = 10

	start := make(chan struct{})
	errs := make(chan error, deliveries)

	for i := 0; i < deliveries; i++ {
		go func() {
			<-start

			resp, err := http.Post(
				srv.URL+"/webhooks/calls",
				"application/json",
				strings.NewReader(body),
			)
			if err != nil {
				errs <- err
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				errs <- fmt.Errorf("got status %d, want 200", resp.StatusCode)
				return
			}

			errs <- nil
		}()
	}

	close(start)

	for i := 0; i < deliveries; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("delivery %d failed: %v", i, err)
		}
	}

	var n int
	err := st.Pool().QueryRow(
		ctx,
		`SELECT count(*) FROM events WHERE event_id = $1`,
		eventID,
	).Scan(&n)
	if err != nil {
		t.Fatalf("count events: %v", err)
	}

	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

func TestDuplicateDeliveryDoesNotDoubleCountStats(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	// Deliver the same event multiple times.
	for i := 0; i < 3; i++ {
		resp := post(t, srv.URL+"/webhooks/calls", body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var callCount int
	var totalDuration int64

	err := st.Pool().QueryRow(
		ctx,
		`SELECT call_count, total_duration_sec
		 FROM account_stats
		 WHERE account_id = $1`,
		accountID,
	).Scan(&callCount, &totalDuration)
	if err != nil {
		t.Fatalf("query account stats: %v", err)
	}

	if callCount != 1 {
		t.Fatalf("call_count = %d, want 1", callCount)
	}

	if totalDuration != 143 {
		t.Fatalf("total_duration_sec = %d, want 153", totalDuration)
	}
}

func TestRecordingIsMarkedProcessed(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	resp := post(t, srv.URL+"/webhooks/calls", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	// Recording processing happens asynchronously and currently waits
	// for recordingWork (50ms).
	time.Sleep(100 * time.Millisecond)

	var processed bool
	err := st.Pool().QueryRow(
		ctx,
		`SELECT recording_processed
		 FROM calls
		 WHERE call_id = $1`,
		callID,
	).Scan(&processed)

	if err != nil {
		t.Fatalf("query recording status: %v", err)
	}

	if !processed {
		t.Fatal("recording was not marked as processed")
	}
}
