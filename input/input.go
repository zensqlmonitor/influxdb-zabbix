package input

import (
	"time"
)

type Input struct {
    Address    string
	Tablename  string
	Rowsperbatch int
	Startdate  time.Time
	Enddate    time.Time
	Result     []string
}
