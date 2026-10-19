// response 包提供通用 API 响应结构。
package response

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pengtor/seckill-agent/pkg/apperror"
)

func Success(c *gin.Context, status int, message string, data gin.H) {
	response := gin.H{"code": 0, "message": message, "request_id": c.GetString("request_id")}
	for key, value := range data {
		response[key] = value
	}
	c.JSON(status, response)
}

func Error(c *gin.Context, err *apperror.Error) {
	status := err.HTTPStatus
	if status == 0 {
		status = http.StatusInternalServerError
	}
	c.JSON(status, gin.H{"code": err.Code, "message": err.Message, "request_id": c.GetString("request_id")})
}
