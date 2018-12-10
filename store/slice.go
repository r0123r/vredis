package store

import (
	"github.com/r0123r/vredis/store/driver"
)

type Slice interface {
	driver.ISlice
}
