package middleware

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

// RateLimiter returns a Gin middleware that allows max requests per window per IP.
// Uses Redis INCR + EXPIRE (sliding counter per window).
func RateLimiter(client *redis.Client, max int, window time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := context.Background()
		key := fmt.Sprintf("rate:%s", c.ClientIP())

		count, err := client.Incr(ctx, key).Result()
		if err != nil {
			// Redis unavailable — let the request through rather than blocking everyone
			c.Next()
			return
		}

		// Set TTL only on the first request in the window
		if count == 1 {
			client.Expire(ctx, key, window)
		}

		if count > int64(max) {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": "rate limit exceeded, try again later",
			})
			return
		}

		c.Next()
	}
}
