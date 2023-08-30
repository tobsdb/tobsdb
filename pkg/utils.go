package pkg

func Filter[T any](items []T, predicate func(T) bool) []T {
	filtered := []T{}
	for _, item := range items {
		if predicate(item) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}
