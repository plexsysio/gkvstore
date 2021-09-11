package tracing_test

import (
	"testing"

	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/plexsysio/gkvstore/inmem"
	"github.com/plexsysio/gkvstore/testsuite"
	"github.com/plexsysio/gkvstore/tracing"
)

func TestSuite(t *testing.T) {
	tracer := mocktracer.New()
	inmemStore := inmem.New()

	testsuite.RunTestsuite(t, tracing.NewTracingStore(inmemStore, tracer), testsuite.Advanced)

	// Following values are based on the testsuite operations
	if len(tracer.FinishedSpans()) != 24 {
		t.Fatal("incorrect no of spans")
	}
	create, read, update, deleteC, list := 0, 0, 0, 0, 0
	for _, v := range tracer.FinishedSpans() {
		if v.OperationName == "Create" {
			create++
		}
		if v.OperationName == "Read" {
			read++
		}
		if v.OperationName == "Update" {
			update++
		}
		if v.OperationName == "Delete" {
			deleteC++
		}
		if v.OperationName == "List" {
			list++
		}
	}
	if create != 11 {
		t.Fatal("create count incorrect", create)
	}
	if read != 3 {
		t.Fatal("read count incorrect", read)
	}
	if update != 1 {
		t.Fatal("update count incorrect", update)
	}
	if deleteC != 1 {
		t.Fatal("delete count incorrect", deleteC)
	}
	if list != 8 {
		t.Fatal("list count incorrect", list)
	}
}
