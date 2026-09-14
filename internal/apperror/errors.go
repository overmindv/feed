package apperror

import "fmt"

const (
	PermissionDenied = "PERMISSION_DENIED"
	InternalError    = "INTERNAL_ERROR"
)

// Error — публичная ошибка с машинным кодом и HTTP status.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Status  int    `json:"-"`
}

// Error возвращает безопасное строковое представление публичной ошибки.
func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// New создаёт публичную ошибку.
func New(code, message string, status int) *Error {
	return &Error{Code: code, Message: message, Status: status}
}
