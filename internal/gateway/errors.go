package gateway

import (
	"fmt"

	"github.com/pengtor/seckill-agent/pkg/apperror"
)

func validationError(message string) *apperror.Error {
	return apperror.New("VALIDATION_ERROR", message, 400, nil)
}

func rateLimitRejected(scope string) *apperror.Error {
	return apperror.New("RATE_LIMITED", fmt.Sprintf("%s rate limit exceeded", scope), 429, nil)
}

func rateLimitUnavailable(err error) *apperror.Error {
	return apperror.New("RATE_LIMIT_UNAVAILABLE", "rate limiter unavailable", 503, err)
}
