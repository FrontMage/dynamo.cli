package utils

func FindIndex(from []string, match string) int {
	for idx, value := range from {
		if value == match {
			return idx
		}
	}
	return -1
}
