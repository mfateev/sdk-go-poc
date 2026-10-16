package jobs

var count = 40

func init() { count++ }

//temporal:workflow
func Entry() (int, error) { count++; return count, nil }

func HostCount() int { return count }
