package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"order-service/internal/domain"
	"order-service/internal/usecase"
)

// CachedOrderRepo wraps any OrderRepository and adds Redis cache-aside logic.
type CachedOrderRepo struct {
	inner  usecase.OrderRepository
	client *redis.Client
	ttl    time.Duration
}

func NewCachedOrderRepo(inner usecase.OrderRepository, client *redis.Client, ttl time.Duration) *CachedOrderRepo {
	return &CachedOrderRepo{inner: inner, client: client, ttl: ttl}
}

func cacheKey(id string) string {
	return fmt.Sprintf("order:%s", id)
}

// FindByID checks Redis first; on miss, fetches from DB and caches the result.
func (r *CachedOrderRepo) FindByID(id string) (*domain.Order, error) {
	ctx := context.Background()
	key := cacheKey(id)

	val, err := r.client.Get(ctx, key).Result()
	if err == nil {
		// Cache hit
		var order domain.Order
		if jsonErr := json.Unmarshal([]byte(val), &order); jsonErr == nil {
			log.Printf("[Cache] HIT  order:%s", id)
			return &order, nil
		}
	}

	// Cache miss — fetch from DB
	log.Printf("[Cache] MISS order:%s", id)
	order, err := r.inner.FindByID(id)
	if err != nil {
		return nil, err
	}

	data, _ := json.Marshal(order)
	r.client.Set(ctx, key, data, r.ttl)
	return order, nil
}

// Save delegates to the inner repo — no cache action needed on create.
func (r *CachedOrderRepo) Save(order *domain.Order) error {
	return r.inner.Save(order)
}

// UpdateStatus updates the DB and immediately deletes the cache key so the
// next read gets fresh data (atomic invalidation).
func (r *CachedOrderRepo) UpdateStatus(id string, status string) error {
	if err := r.inner.UpdateStatus(id, status); err != nil {
		return err
	}
	r.client.Del(context.Background(), cacheKey(id))
	log.Printf("[Cache] INVALIDATED order:%s (status→%s)", id, status)
	return nil
}
