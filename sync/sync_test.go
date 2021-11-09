package syncstore_test

import (
	"testing"

	"github.com/plexsysio/gkvstore/inmem"
	"github.com/plexsysio/gkvstore/sync"
	"github.com/plexsysio/gkvstore/testsuite"
)

func TestSuite(t *testing.T) {
	testsuite.RunTestsuite(t, syncstore.New(inmem.New()), testsuite.Advanced)
}

func BenchmarkSuite(b *testing.B) {
	testsuite.BenchmarkSuite(b, syncstore.New(inmem.New()))
}
