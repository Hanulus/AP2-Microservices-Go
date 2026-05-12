package http

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"order-service/internal/transport/http/middleware"
)

// NewRouter registers all routes for the Order Service.
// redisClient and rate limit params are used for the bonus rate-limiter middleware.
func NewRouter(h *OrderHandler, redisClient *redis.Client, maxReq int, window time.Duration) *gin.Engine {
	r := gin.Default()

	r.Use(middleware.RateLimiter(redisClient, maxReq, window))

	r.POST("/orders", h.CreateOrder)
	r.GET("/orders/:id", h.GetOrder)
	r.PATCH("/orders/:id/cancel", h.CancelOrder)

	return r
}
