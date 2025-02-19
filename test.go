package cache

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"

	"ep-services/pkg/lib/log"
	"ep-services/utils"
)

type GetValueFunc[T any] func() (map[string]T, error)

type CacheHandler[T any] struct {
	Cache        ICache
	CacheKey     string          // 缓存Key
	Expiration   time.Duration   // 过期时间
	GetValueFunc GetValueFunc[T] // 获取数据func
}

func NewCacheHandler[T any](cache ICache, cacheKey string, expiration time.Duration, fn GetValueFunc[T]) (*CacheHandler[T], error) {
	if fn == nil {
		return nil, errors.New("no get entity func")
	}
	return &CacheHandler[T]{
		Cache:        cache,
		CacheKey:     cacheKey,
		Expiration:   expiration,
		GetValueFunc: fn,
	}, nil
}

// 获取缓存中的数据，如果没有则设置到缓存中并返回
func (c *CacheHandler[T]) GetValueFromCache(subkeys ...string) ([]T, error) {
	value, err := c.getCache(subkeys...)
	if err != nil || value == nil {
		// 加载数据
		values, err := c.setToCacheAndGetValue()
		if err != nil {
			return nil, err
		}
		return values, nil
	}
	return value, nil
}

// 缓存失效存入缓存，这里直接返回所有值
// 后面从缓存获取只返回subkey对应值
func (c *CacheHandler[T]) setToCacheAndGetValue() ([]T, error) {
	valueMap, err := c.GetValueFunc()
	if err != nil {
		return nil, err
	}
	newMap := make(map[string]string)
	for k, v := range valueMap {
		v_ := v
		b, err := json.Marshal(&v_)
		if err != nil {
			return nil, err
		}
		newMap[k] = string(b)
	}
	c.setCache(newMap)
	return utils.MapValues(valueMap), nil
}

func (c *CacheHandler[T]) setCache(value map[string]string) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if err := c.Cache.HashSetEx(ctx, c.CacheKey, value, c.Expiration); err != nil {
		log.Error().Msgf("set cache error : %s", err)
	}
}

func (c *CacheHandler[T]) getCache(subkeys ...string) ([]T, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	// 如果没有subkeys那么返回所有数据
	var values []string
	var err error
	if len(subkeys) == 0 {
		values, err = c.getAllValue(ctx)
	} else {
		values, err = c.getValue(ctx, subkeys...)
	}
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		log.Error().Msgf("get cache error : %s", err)
		return nil, err
	}

	if len(values) == 0 {
		return nil, nil
	}

	valMap := []T{}
	for _, v := range values {
		var t T
		err := json.Unmarshal([]byte(v), &t)
		if err != nil {
			return nil, err
		}
		valMap = append(valMap, t)
	}
	return valMap, nil
}

func (c *CacheHandler[T]) getAllValue(ctx context.Context) ([]string, error) {
	valueMap, err := c.Cache.HashGetAll(ctx, c.CacheKey)
	if err != nil {
		return nil, err
	}
	values := utils.MapValues(valueMap)
	return values, nil
}

func (c *CacheHandler[T]) getValue(ctx context.Context, subkeys ...string) ([]string, error) {
	values, err := c.Cache.HashMGet(ctx, c.CacheKey, subkeys...)
	if err != nil {
		return nil, err
	}
	valArr := []string{}
	for _, v := range values {
		if v_, ok := v.(string); ok {
			valArr = append(valArr, v_)
		}
	}
	return valArr, nil
}
