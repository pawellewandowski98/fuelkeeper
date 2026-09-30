package aerospike

import (
	"container/heap"
	"sort"
)

// boundedTop keeps the limit smallest items (by less) pushed into it, in
// O(limit) memory: a max-heap whose root is the largest item kept, evicted
// whenever a smaller item arrives once the heap is full. It lets a query
// whose results arrive unordered select an ordered page without holding the
// whole result set.
type boundedTop[T any] struct {
	limit int
	h     maxHeap[T]
}

func newBoundedTop[T any](limit int, less func(a, b T) bool) *boundedTop[T] {
	return &boundedTop[T]{limit: limit, h: maxHeap[T]{less: less}}
}

func (b *boundedTop[T]) push(x T) {
	if b.limit <= 0 {
		return
	}
	if len(b.h.items) < b.limit {
		heap.Push(&b.h, x)
		return
	}
	if b.h.less(x, b.h.items[0]) {
		b.h.items[0] = x
		heap.Fix(&b.h, 0)
	}
}

// sorted returns the kept items in ascending order.
func (b *boundedTop[T]) sorted() []T {
	out := append(make([]T, 0, len(b.h.items)), b.h.items...)
	sort.Slice(out, func(i, j int) bool { return b.h.less(out[i], out[j]) })
	return out
}

type maxHeap[T any] struct {
	items []T
	less  func(a, b T) bool
}

func (h *maxHeap[T]) Len() int           { return len(h.items) }
func (h *maxHeap[T]) Less(i, j int) bool { return h.less(h.items[j], h.items[i]) }
func (h *maxHeap[T]) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *maxHeap[T]) Push(x any)         { h.items = append(h.items, x.(T)) } //nolint:forcetypeassert // heap.Interface
func (h *maxHeap[T]) Pop() any {
	n := len(h.items)
	x := h.items[n-1]
	h.items = h.items[:n-1]
	return x
}
