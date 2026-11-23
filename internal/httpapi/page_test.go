package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/overmindv/feed/internal/domain"
	"github.com/overmindv/feed/internal/repository"
	"github.com/overmindv/feed/internal/service"
)

const cursorFixtureID = "12345678-abcd-1234-abcd-123456789012"

func TestFeedCursorRoundTrip(t *testing.T) {
	t.Parallel()

	for _, material := range []bool{false, true} {
		original := domain.FeedCursor{
			OccurredAt: time.Date(2026, 11, 23, 10, 0, 0, 123456000, time.UTC),
			ID:         cursorFixtureID, Material: material,
		}
		decoded, ok := decodeCursor(encodeCursor(original))
		if !ok || decoded == nil || *decoded != original {
			t.Fatal("cursor did not preserve its key:", decoded)
		}
	}
	if got, ok := decodeCursor(""); !ok || got != nil {
		t.Fatal("empty cursor must request the first page")
	}
	for _, raw := range []string{"2:1:" + cursorFixtureID + ":f", "1:+1:" + cursorFixtureID + ":f",
		"1:9223372036854775807:" + cursorFixtureID + ":f", "1:2:invalid:m", "1:2:" + cursorFixtureID + ":x"} {
		if _, ok := decodeCursor(base64.RawURLEncoding.EncodeToString([]byte(raw))); ok {
			t.Fatal("accepted malformed cursor:", raw)
		}
	}
}

type pageStore struct {
	repository.Repository
	offset int
	after  *domain.FeedCursor
	limit  int
	reads  int
}

func (s *pageStore) ListItems(_ context.Context, limit, offset int) ([]domain.FeedItem, error) {
	s.offset, s.limit = offset, limit
	s.reads++

	return nil, nil
}

func (s *pageStore) ListPage(_ context.Context, limit int, after *domain.FeedCursor) (domain.FeedPage, error) {
	s.limit, s.after = limit, after
	s.reads++

	return domain.FeedPage{Next: &domain.FeedCursor{ID: cursorFixtureID, OccurredAt: time.UnixMicro(123).UTC()}}, nil
}

func TestFeedPageRoutes(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	store := &pageStore{}
	token := strings.Repeat("f", 32)
	Register(mux, service.New(store), token, nil)
	request := func(query string, credential string, expected int) *httptest.ResponseRecorder {
		t.Helper()

		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/feed"+query, nil)
		req.Header.Set(serviceTokenHeader, credential)
		res := httptest.NewRecorder()
		mux.ServeHTTP(res, req)
		if res.Code != expected || res.Header().Get("Cache-Control") != "no-store" {
			t.Fatal(query, "unexpected response:", res.Code, res.Body.String())
		}

		return res
	}
	request("?cursor=", "wrong", http.StatusForbidden)
	for _, query := range []string{"?cursor=%ZZ", "?cursor=!bad", "?cursor=&cursor=", "?cursor=&offset=0",
		"?cursor=&limit=0", "?cursor=&limit=101", "?cursor=&limit=10&limit=10", "?cursor=" + strings.Repeat("a", maxCursorSize+1)} {
		request(query, token, http.StatusBadRequest)
	}
	if store.reads != 0 {
		t.Fatal("invalid cursor reached the store")
	}

	legacy := request("?limit=11&offset=2", token, http.StatusOK)
	if strings.Contains(legacy.Body.String(), "nextCursor") || store.limit != 11 || store.offset != 2 {
		t.Fatal("legacy response changed:", legacy.Body.String())
	}
	first := request("?limit=1&cursor=", token, http.StatusOK)
	var page listResponse
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil || store.after != nil || store.limit != 1 || page.NextCursor == "" {
		t.Fatal("first cursor page was not returned:", first.Body.String(), err)
	}
	request("?cursor="+page.NextCursor, token, http.StatusOK)
	if store.after == nil || store.after.ID != cursorFixtureID || store.after.OccurredAt.UnixMicro() != 123 || store.limit != domain.DefaultPageSize {
		t.Fatal("next cursor did not reach the store:", store.after)
	}
}
