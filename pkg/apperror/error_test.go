package apperror

import (
	"errors"
	"net/http"
	"testing"
)

func TestInternalDoesNotExposeCause(t *testing.T) {
	cause := errors.New("database password leaked")
	err := Internal(cause)
	if err.Message != "internal server error" || err.HTTPStatus != http.StatusInternalServerError {
		t.Fatalf("unexpected public error: %#v", err)
	}
	if !errors.Is(err, cause) {
		t.Fatal("wrapped cause not preserved")
	}
}
