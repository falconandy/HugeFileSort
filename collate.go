package main

import (
	"sync"

	"golang.org/x/text/collate"
)

func NewCollatorPool(factory func() *collate.Collator) *CollatorPool {
	if factory == nil {
		return &CollatorPool{}
	}

	return &CollatorPool{
		pool: &sync.Pool{
			New: func() any {
				return factory()
			},
		},
	}
}

type CollatorPool struct {
	pool *sync.Pool
}

func (cp *CollatorPool) Get() *collate.Collator {
	if cp.pool != nil {
		return cp.pool.Get().(*collate.Collator)
	}
	return nil
}

func (cp *CollatorPool) Put(collator *collate.Collator) {
	if cp.pool != nil {
		cp.pool.Put(collator)
	}
}
