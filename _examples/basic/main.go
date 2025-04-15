package main

import (
	"context"
	"fmt"
	"time"

	memcache "github.com/goware/cachestore-mem"
	cachestore "github.com/goware/cachestore2"
	"github.com/goware/logger"
	"github.com/goware/pubsub/membus"

	fluxcache "github.com/goware/cachestore-flux"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	backend1, err := memcache.NewBackend(200)
	if err != nil {
		panic(err)
	}

	store1 := cachestore.OpenStore[string](
		backend1,
		cachestore.WithDefaultKeyExpiry(2*time.Second),
	)

	backend2, err := memcache.NewBackend(200)
	if err != nil {
		panic(err)
	}

	store2 := cachestore.OpenStore[string](
		backend2,
		cachestore.WithDefaultKeyExpiry(2*time.Second),
	)

	log := logger.NewLogger(logger.LogLevel_DEBUG)
	ps, err := membus.New[fluxcache.StoreInvalidationMessage](log)
	if err != nil {
		panic(err)
	}
	runErr := make(chan error, 1)
	go func() {
		runErr <- ps.Run(context.Background())
	}()

	icache1 := fluxcache.NewFluxStore[string](store1, ps)
	icache2 := fluxcache.NewFluxStore[string](store2, ps)

	invalidator := fluxcache.NewStoreInvalidator(log, icache2, ps)
	go func() {
		if err := invalidator.Listen(ctx); err != nil {
			panic(err)
		}
	}()

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("foo:%d", i)
		val := fmt.Sprintf("value-%d", i)
		if err := icache1.Set(ctx, key, val); err != nil {
			panic(err)
		}
	}
	if err := icache1.SetEx(ctx, "foo:999", "value-999", 10*time.Minute); err != nil {
		panic(err)
	}

	if err := icache2.SetEx(ctx, "foo:999", "value-999", 10*time.Minute); err != nil {
		panic(err)
	}

	v, ok, err := icache1.Get(ctx, "foo:1")
	if err != nil {
		panic(err)
	}
	if !ok {
		panic("foo:1 was not found unexpectedly")
	}
	fmt.Println("=> [store1] get(foo:1) =", v)

	time.Sleep(3 * time.Second)

	// "foo:1" should expire
	v, ok, err = icache1.Get(ctx, "foo:1")
	if err != nil {
		panic(err)
	}
	fmt.Printf("=> [store1] after 3s, get(foo:1) => ok=%v, val=%v\n", ok, v)

	// "foo:999" should stay
	v, ok, err = icache1.Get(ctx, "foo:999")
	if err != nil {
		panic(err)
	}
	fmt.Printf("=> [store1] get(foo:999) => ok=%v, val=%v\n", ok, v)

	v, ok, err = icache2.Get(ctx, "foo:999")
	if err != nil {
		panic(err)
	}
	fmt.Printf("=> [store2] get(foo:999) => ok=%v, val=%v\n", ok, v)

	if err := icache1.DeletePrefix(ctx, "foo:"); err != nil {
		panic(err)
	}
	fmt.Println("=> prefix deletion on 'foo:' done")

	v, ok, _ = icache1.Get(ctx, "foo:999")
	if ok {
		panic("unexpected: foo:999 should be deleted by prefix")
	}
	fmt.Printf("=> [store1] get(foo:999) => ok=%v, val=%v\n", ok, v)

	// wait for the invalidator to process the message
	time.Sleep(1 * time.Second)

	v, ok, _ = icache2.Get(ctx, "foo:999")
	if ok {
		panic("unexpected: foo:999 should be deleted by store invalidator")
	}
	fmt.Printf("=> [store2] get(foo:999) => ok=%v, val=%v\n", ok, v)

	ps.Stop()

	err = <-runErr
	if err != nil {
		panic(err)
	}

	fmt.Println("All done.")
}
