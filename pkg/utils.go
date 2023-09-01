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

// Converts a value suspected to be either an int or float64 to an int.
// This kind of logic is used all over the code(due to json decoding all numbers as float64) so it's kinda needed
func NumToInt(num any) int {
	switch num := num.(type) {
	case int:
		return num
	case float64:
		return int(num)
	}
	return 0
}
