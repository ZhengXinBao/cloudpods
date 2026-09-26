package syncworker

// RoutesAccount defaults to legacy execution, including an empty canary list.
func RoutesAccount(enabled bool, accounts []string, id string) bool {
	if !enabled || id == "" {
		return false
	}
	for _, account := range accounts {
		if account == id || account == "*" {
			return true
		}
	}
	return false
}
