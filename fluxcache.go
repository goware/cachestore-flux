package fluxcache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	cachestore "github.com/goware/cachestore2"
	"github.com/goware/pubsub"
)

const (
	DefaultChannelID = "cachestore-flux"
)

type InstanceID string

func newInstanceID() InstanceID {
	return InstanceID(uuid.NewString())
}

type CacheInvalidationEntry struct {
	Key         string `json:"key"`
	ContentHash string `json:"content_hash,omitempty"`
}

type StoreInvalidationMessage struct {
	Entries []CacheInvalidationEntry `json:"entries"`
	Origin  InstanceID               `json:"origin"`
}

type LocalFluxStore interface {
	GetInstanceID() InstanceID
	GetAny(ctx context.Context, key string) (any, bool, error)

	DeleteLocal(ctx context.Context, key string) error
	DeletePrefixLocal(ctx context.Context, prefix string) error
	ClearAllLocal(ctx context.Context) error
}

type FluxStore[V any] struct {
	store      cachestore.Store[V]
	pubsub     pubsub.PubSub[StoreInvalidationMessage]
	instanceID InstanceID
}

func NewFluxStore[V any](store cachestore.Store[V], ps pubsub.PubSub[StoreInvalidationMessage]) *FluxStore[V] {
	return &FluxStore[V]{
		store:      store,
		pubsub:     ps,
		instanceID: newInstanceID(),
	}
}

func (ic *FluxStore[V]) Name() string {
	return ic.store.Name()
}

func (ic *FluxStore[V]) Options() cachestore.StoreOptions {
	return ic.store.Options()
}

func (ic *FluxStore[V]) Exists(ctx context.Context, key string) (bool, error) {
	return ic.store.Exists(ctx, key)
}

func (ic *FluxStore[V]) Get(ctx context.Context, key string) (V, bool, error) {
	return ic.store.Get(ctx, key)
}

func (ic *FluxStore[V]) BatchGet(ctx context.Context, keys []string) ([]V, []bool, error) {
	return ic.store.BatchGet(ctx, keys)
}

func (ic *FluxStore[V]) Set(ctx context.Context, key string, value V) error {
	if err := ic.store.Set(ctx, key, value); err != nil {
		return err
	}
	hash, err := ComputeHash(value)
	if err != nil {
		hash = ""
	}
	return ic.publishInvalidation(ctx, []CacheInvalidationEntry{{Key: key, ContentHash: hash}})
}

func (ic *FluxStore[V]) SetEx(ctx context.Context, key string, value V, ttl time.Duration) error {
	if err := ic.store.SetEx(ctx, key, value, ttl); err != nil {
		return err
	}
	hash, err := ComputeHash(value)
	if err != nil {
		hash = ""
	}
	return ic.publishInvalidation(ctx, []CacheInvalidationEntry{{Key: key, ContentHash: hash}})
}

func (ic *FluxStore[V]) BatchSet(ctx context.Context, keys []string, values []V) error {
	if err := ic.store.BatchSet(ctx, keys, values); err != nil {
		return err
	}
	entries := make([]CacheInvalidationEntry, len(keys))
	for i, key := range keys {
		hash, err := ComputeHash(values[i])
		if err != nil {
			hash = ""
		}
		entries[i] = CacheInvalidationEntry{
			Key:         key,
			ContentHash: hash,
		}
	}
	return ic.publishInvalidation(ctx, entries)
}

func (ic *FluxStore[V]) BatchSetEx(
	ctx context.Context,
	keys []string,
	values []V,
	ttl time.Duration,
) error {
	if err := ic.store.BatchSetEx(ctx, keys, values, ttl); err != nil {
		return err
	}
	entries := make([]CacheInvalidationEntry, len(keys))
	for i, key := range keys {
		hash, err := ComputeHash(values[i])
		if err != nil {
			hash = ""
		}
		entries[i] = CacheInvalidationEntry{
			Key:         key,
			ContentHash: hash,
		}
	}
	return ic.publishInvalidation(ctx, entries)
}

func (ic *FluxStore[V]) Delete(ctx context.Context, key string) error {
	if err := ic.DeleteLocal(ctx, key); err != nil {
		return err
	}
	return ic.publishInvalidation(ctx, []CacheInvalidationEntry{{Key: key}})
}

func (ic *FluxStore[V]) DeleteLocal(ctx context.Context, key string) error {
	if err := ic.store.Delete(ctx, key); err != nil {
		return err
	}
	return nil
}

func (ic *FluxStore[V]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	if err := ic.DeletePrefixLocal(ctx, keyPrefix); err != nil {
		return err
	}
	return ic.publishInvalidation(ctx, []CacheInvalidationEntry{{Key: fmt.Sprintf("%s*", keyPrefix)}})
}

func (ic *FluxStore[V]) DeletePrefixLocal(ctx context.Context, keyPrefix string) error {
	if err := ic.store.DeletePrefix(ctx, keyPrefix); err != nil {
		return err
	}
	return nil
}

func (ic *FluxStore[V]) ClearAll(ctx context.Context) error {
	if err := ic.ClearAllLocal(ctx); err != nil {
		return err
	}
	return ic.publishInvalidation(ctx, []CacheInvalidationEntry{{Key: "*"}})
}

func (ic *FluxStore[V]) ClearAllLocal(ctx context.Context) error {
	if err := ic.store.ClearAll(ctx); err != nil {
		return err
	}
	return nil
}

func (ic *FluxStore[V]) GetOrSetWithLock(ctx context.Context, key string, getter func(context.Context, string) (V, error)) (V, error) {
	var zero V

	wrappedGetter, wasCalled := wrapGetterWasCalled(getter)
	value, err := ic.store.GetOrSetWithLock(ctx, key, wrappedGetter)
	if err != nil {
		return zero, err
	}

	if *wasCalled {
		hash, err := ComputeHash(value)
		if err != nil {
			hash = ""
		}
		if err := ic.publishInvalidation(ctx, []CacheInvalidationEntry{{Key: key, ContentHash: hash}}); err != nil {
			return value, err
		}
	}
	return value, nil
}

func (ic *FluxStore[V]) GetOrSetWithLockEx(ctx context.Context, key string, getter func(context.Context, string) (V, error), ttl time.Duration) (V, error) {
	var zero V

	wrappedGetter, wasCalled := wrapGetterWasCalled(getter)
	value, err := ic.store.GetOrSetWithLockEx(ctx, key, wrappedGetter, ttl)
	if err != nil {
		return zero, err
	}

	if *wasCalled {
		hash, err := ComputeHash(value)
		if err != nil {
			hash = ""
		}
		if err := ic.publishInvalidation(ctx, []CacheInvalidationEntry{{Key: key, ContentHash: hash}}); err != nil {
			return value, err
		}
	}
	return value, nil
}

func (ts FluxStore[V]) GetAny(ctx context.Context, key string) (any, bool, error) {
	val, ok, err := ts.Get(ctx, key)
	if err != nil {
		return nil, false, err
	}

	return val, ok, nil
}

func (ic FluxStore[V]) GetInstanceID() InstanceID {
	return ic.instanceID
}

func wrapGetterWasCalled[V any](
	getter func(context.Context, string) (V, error),
) (wrapped func(context.Context, string) (V, error), wasCalled *bool) {
	var called bool

	wrapped = func(ctx context.Context, key string) (V, error) {
		called = true
		return getter(ctx, key)
	}
	return wrapped, &called
}

// Publish invalidation event
func (ic *FluxStore[V]) publishInvalidation(ctx context.Context, entries []CacheInvalidationEntry) error {
	msg := StoreInvalidationMessage{
		Entries: entries,
		Origin:  ic.instanceID, // we need this to skip invalidating the store for the same key from the same instance
	}
	return ic.pubsub.Publish(ctx, DefaultChannelID, msg)
}

func ComputeHash[V any](v V) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("failed to marshal data: %w", err)
	}

	sum := sha256.Sum256(data)

	return hex.EncodeToString(sum[:]), nil
}
