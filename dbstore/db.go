package dbstore

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PoolManager struct {
	pools sync.Map // Map to hold pools with app_id as the key
	ttl   time.Duration
}

type PoolInfo struct {
	pool     *pgxpool.Pool
	lastUsed time.Time
}

// NewPoolManager initializes a new PoolManager
func NewPoolManager(ttl time.Duration) *PoolManager {
	manager := &PoolManager{
		ttl: ttl,
	}
	go manager.pruneInactivePools()
	return manager
}

// GetPool retrieves or initializes a connection pool for the given app_id
func (pm *PoolManager) GetPool(appID string) (*pgxpool.Pool, error) {
	now := time.Now()
	if value, ok := pm.pools.Load(appID); ok {
		info := value.(*PoolInfo)
		info.lastUsed = now
		return info.pool, nil
	}

	// Create a new pool
	connString := "postgres://bhargav:Bhargav123@localhost:5432/" + appID
	pool, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		return nil, err
	}

	pm.pools.Store(appID, &PoolInfo{
		pool:     pool,
		lastUsed: now,
	})

	log.Printf("Initialized new connection pool for app_id: %s\n", appID)
	return pool, nil
}

// pruneInactivePools periodically checks and removes inactive pools
func (pm *PoolManager) pruneInactivePools() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		pm.pools.Range(func(key, value interface{}) bool {
			info := value.(*PoolInfo)
			if now.Sub(info.lastUsed) > pm.ttl {
				info.pool.Close()
				pm.pools.Delete(key)
				log.Printf("Pruned inactive pool for app_id: %s\n", key)
			}
			return true
		})
	}
}

var GlobalPoolManager *PoolManager

// Initialize the global pool manager
func InitPoolManager(ttl time.Duration) {
	GlobalPoolManager = NewPoolManager(ttl)
}
