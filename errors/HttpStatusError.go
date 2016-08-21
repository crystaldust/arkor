package errors

import "strconv"

// HttpStatusError Any http status that is NOT 200
type HttpStatusError struct {
	Status int
}

func (e HttpStatusError) Error() string {
	return strconv.Itoa(e.Status)
}
