package set

type set[T comparable] struct {
	elements []T
}

func FromSlice[T comparable](s []T) set[T] {
	return set[T]{elements: append([]T{}, s...)}
}

func (s set[T]) Equal(target set[T]) bool {
	b := target.elements

	if len(s.elements) != len(b) {
		return false
	}

	for _, av := range s.elements {
		for bi, bv := range b {
			if av == bv {
				b = append(b[:bi], b[bi+1:]...)
				break
			}
		}
	}

	return len(b) == 0
}
