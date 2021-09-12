package inmem_test

import (
	"testing"

	"github.com/plexsysio/gkvstore/inmem"
	"github.com/plexsysio/gkvstore/testsuite"
)

func TestSuite(t *testing.T) {
	testsuite.RunTestsuite(t, inmem.New(), testsuite.Advanced)
}

func BenchmarkSuite(b *testing.B) {
	testsuite.BenchmarkSuite(b, inmem.New())
}
