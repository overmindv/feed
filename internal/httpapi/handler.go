package httpapi

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/overmindv/feed/internal/apperror"
	"github.com/overmindv/feed/internal/service"
)

const serviceTokenHeader = "X-Feed-Service-Token"

// Handler — внутренний HTTP API ленты.
type Handler struct {
	service      *service.Service
	logger       *slog.Logger
	gatewayToken string
}

// item — DTO записи ленты для ответа.
type item struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	Title       string `json:"title"`
	Text        string `json:"text"`
	Href        string `json:"href"`
	ActorUserID string `json:"actorUserId"`
	OccurredAt  string `json:"occurredAt"`
}

// listResponse — ответ GET /v1/feed.
type listResponse struct {
	Items  []item `json:"items"`
	Limit  int    `json:"limit"`
	Offset int    `json:"offset"`
}

// Router описывает минимальный контракт HTTP-роутера.
type Router interface {
	Handle(pattern string, handler http.Handler)
	HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))
}

// Register регистрирует токен-защищённый внутренний API ленты.
// Liveness/readiness/metrics/middleware предоставляет parker.
func Register(router Router, feed *service.Service, gatewayToken string, logger *slog.Logger) {
	h := &Handler{service: feed, logger: logger, gatewayToken: gatewayToken}
	router.Handle("GET /v1/feed", h.auth(http.HandlerFunc(h.list)))
}

// auth проверяет X-Feed-Service-Token (gateway → feed).
func (h *Handler) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get(serviceTokenHeader)
		if len(token) != len(h.gatewayToken) || subtle.ConstantTimeCompare([]byte(token), []byte(h.gatewayToken)) != 1 {
			writeJSON(w, http.StatusForbidden, apperror.New(apperror.PermissionDenied, "внутренний вызов не авторизован", http.StatusForbidden))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// list отдаёт записи ленты по убыванию времени события.
func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	limit := parseQueryInt(r, "limit", 50)
	offset := parseQueryInt(r, "offset", 0)

	items, err := h.service.ListFeed(r.Context(), limit, offset)
	if err != nil {
		h.writeError(w, err)
		return
	}

	out := make([]item, 0, len(items))
	for _, it := range items {
		out = append(out, item{
			ID:          it.ID,
			Kind:        it.Kind,
			Title:       it.Title,
			Text:        it.Text,
			Href:        it.Href,
			ActorUserID: it.ActorUserID,
			OccurredAt:  it.OccurredAt.Format("2006-01-02T15:04:05Z07:00"),
		})
	}
	writeJSON(w, http.StatusOK, listResponse{Items: out, Limit: limit, Offset: offset})
}

func (h *Handler) writeError(w http.ResponseWriter, err error) {
	var public *apperror.Error
	if errors.As(err, &public) {
		writeJSON(w, public.Status, public)
		return
	}
	h.logger.Error("внутренняя ошибка Feed", "error", err)
	writeJSON(w, http.StatusInternalServerError, apperror.New(apperror.InternalError, "внутренняя ошибка", http.StatusInternalServerError))
}

// parseQueryInt читает int из query-параметра с fallback.
func parseQueryInt(r *http.Request, key string, fallback int) int {
	value := r.URL.Query().Get(key)
	if value == "" {
		return fallback
	}
	number, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}

	return number
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
