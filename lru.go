package httpcache

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	lru "github.com/hashicorp/golang-lru"
)

type CacheObj struct {
	body    []byte
	headers http.Header
	code    int
}

func (c CacheObj) Size() uint64 {
	// TODO include headers size?
	return uint64(len(c.body))
}

// lruCache fulfills the `httpcache.Cache` interface
type lruCache struct {
	cache *lru.Cache
	stale map[string]time.Time
}

// NewLRUCache returns an ephemeral cache in memory
func NewLRUCache(sizeBytes uint64) (Cache, error) {
	c, err := lru.NewLargeWithEvict(sizeBytes, nil)
	if err != nil {
		return nil, err
	}
	return &lruCache{cache: c}, nil
}

// Retrieve the Status and Headers for a given key path
func (c *lruCache) Header(key string) (Header, error) {
	fmt.Println("lruCache.Header %v\n", key)
	ival, ok := c.cache.Get(key)
	if !ok {
		return Header{}, ErrNotFoundInCache
	}

	val, ok := ival.(CacheObj)
	if !ok {
		return Header{}, fmt.Errorf("cache held unknown value type '%T' in key '%s'", ival, key)
	}
	return Header{Header: val.headers, StatusCode: val.code}, nil
}

// Store a resource against a number of keys
func (c *lruCache) Store(res *Resource, keys ...string) error {
	fmt.Println("lruCache.Store %v\n", keys)
	bytes := []byte(nil)
	if len, err := strconv.ParseInt(res.Header().Get("Content-Length"), 10, 64); err == nil {
		bytes = make([]byte, len)
		n, err := res.Read(bytes)
		if err != nil {
			return err
		}
		if int64(n) != len {
			return err // TODO warn and succeed?
		}
	} else if bytes, err = ioutil.ReadAll(res); err != nil {
		// TODO warn? Missing content-length is an RFC violation.
		return err
	}

	val := CacheObj{
		body:    bytes,
		headers: res.Header(),
		code:    res.statusCode,
	}

	for _, key := range keys {
		fmt.Println("lruCache.Store storing %v {{%v}}\n", key, string(val.body))
		delete(c.stale, key)
		c.cache.AddSize(key, val, val.Size())
	}

	return nil
}

// Retrieve returns a cached Resource for the given key
func (c *lruCache) Retrieve(key string) (*Resource, error) {
	// TODO this is called with the path, but not FDQN. Key must be the full URL, with an option to consider the Query String.
	fmt.Printf("lruCache.Retrieve %v\n", key)
	ival, ok := c.cache.Get(key)
	if !ok {
		fmt.Printf("lruCache.Retrieve c.cache.Get %v !ok\n", key)
		return nil, ErrNotFoundInCache
	}
	fmt.Printf("lruCache.Retrieve c.cache.Get %v ok\n", key)
	val, ok := ival.(CacheObj)
	if !ok {
		return nil, fmt.Errorf("cache had unknown value type '%T' in key '%s'", ival, key)
	}
	fmt.Printf("lruCache.Retrieve c.cache.Get %v val ok\n, key")
	res := NewResourceBytes(val.code, val.body, val.headers)
	if staleTime, exists := c.stale[key]; exists {
		if !res.DateAfter(staleTime) {
			log.Printf("stale marker of %s found", staleTime)
			res.MarkStale()
		}
	}
	fmt.Printf("lruCache.Retrieve c.cache.Get %v returning res\n", key)
	return res, nil
}

// updateHeader updates the header of an existing cached value. This is more efficient than Store(Retrieve()) because it doesn't copy the body bytes.
func (c *lruCache) updateHeader(key string, headers http.Header, code int) error {
	ival, ok := c.cache.Get(key)
	if !ok {
		return ErrNotFoundInCache
	}
	val, ok := ival.(CacheObj)
	if !ok {
		return fmt.Errorf("cache had unknown value of type '%T' in key '%s'", ival, key)
	}
	val.headers = headers
	val.code = code
	c.cache.AddSize(key, val, val.Size())
	return nil
}

func (c *lruCache) Invalidate(keys ...string) {
	log.Printf("invalidating %q", keys)
	for _, key := range keys {
		c.stale[key] = Clock()
	}
}

func (c *lruCache) Freshen(res *Resource, keys ...string) error {
	fmt.Println("lruCache.Freshen %v\n", keys)
	for _, key := range keys {
		if h, err := c.Header(key); err == nil {
			if h.StatusCode == res.Status() && headersEqual(h.Header, res.Header()) {
				debugf("freshening key %s", key)
				if err := c.updateHeader(key, res.Header(), h.StatusCode); err != nil {
					return err
				}
			} else {
				debugf("freshen failed, invalidating %s", key)
				c.Invalidate(key)
			}
		}
	}
	return nil
}
