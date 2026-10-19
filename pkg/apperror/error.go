package apperror

import "net/http"

type Error struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	HTTPStatus int    `json:"-"`
	Err        error  `json:"-"`
}

func (e *Error) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return e.Message
}

func (e *Error) Unwrap() error { return e.Err }

func New(code, message string, status int, err error) *Error {
	return &Error{Code: code, Message: message, HTTPStatus: status, Err: err}
}

func Internal(err error) *Error {
	return New("INTERNAL_ERROR", "internal server error", http.StatusInternalServerError, err)
}
