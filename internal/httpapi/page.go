package httpapi

import (
	"encoding/base64"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/overmindv/feed/internal/apperror"
	"github.com/overmindv/feed/internal/domain"
)

const (
	cursorParam   = "cursor"
	limitParam    = "limit"
	offsetParam   = "offset"
	cursorVersion = "1"
	maxCursorSize = 88
)

var cursorID = regexp.MustCompile(`^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$`)

func encodeCursor(cursor domain.FeedCursor) string {
	branch := "f"
	if cursor.Material {
		branch = "m"
	}
	fields := []string{cursorVersion, strconv.FormatInt(cursor.OccurredAt.UnixMicro(), 10), cursor.ID, branch}

	return base64.RawURLEncoding.EncodeToString([]byte(strings.Join(fields, ":")))
}

func decodeCursor(value string) (*domain.FeedCursor, bool) {
	if value == "" {
		return nil, true
	}
	if len(value) > maxCursorSize || strings.ContainsAny(value, "\r\n") {
		return nil, false
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(value)
	if err != nil {
		return nil, false
	}
	fields := strings.SplitN(string(raw), ":", 4)
	if len(fields) != 4 || fields[0] != cursorVersion || !cursorID.MatchString(fields[2]) ||
		(fields[3] != "f" && fields[3] != "m") {
		return nil, false
	}
	micros, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || strconv.FormatInt(micros, 10) != fields[1] {
		return nil, false
	}
	timestamp := time.UnixMicro(micros).UTC()
	if year := timestamp.Year(); year < 1 || year > 9999 {
		return nil, false
	}

	return &domain.FeedCursor{OccurredAt: timestamp, ID: fields[2], Material: fields[3] == "m"}, true
}

func (h *Handler) listPage(w http.ResponseWriter, r *http.Request, query url.Values) {
	limit := domain.DefaultPageSize
	if values, ok := query[limitParam]; ok {
		limit = parseQueryInt(query, limitParam, 0)
		if len(values) != 1 || limit < 1 || limit > domain.MaxPageSize {
			badPage(w)

			return
		}
	}
	values := query[cursorParam]
	if len(values) != 1 || query.Has(offsetParam) {
		badPage(w)

		return
	}
	after, ok := decodeCursor(values[0])
	if !ok {
		badPage(w)

		return
	}

	page, err := h.service.ListPage(r.Context(), limit, after)
	if err != nil {
		h.writeError(w, err)

		return
	}
	response := listResponse{Items: cards(page.Items), Limit: limit}
	if page.Next != nil {
		response.NextCursor = encodeCursor(*page.Next)
	}
	writeJSON(w, http.StatusOK, response)
}

func badPage(w http.ResponseWriter) {
	writeJSON(w, http.StatusBadRequest, apperror.New(apperror.ValidationError, "некорректные параметры страницы ленты", http.StatusBadRequest))
}
