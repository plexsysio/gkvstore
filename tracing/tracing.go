package tracing

import (
	"context"
	"github.com/opentracing/opentracing-go"
	tlog "github.com/opentracing/opentracing-go/log"
	"github.com/plexsysio/gkvstore"
)

type contextKey struct{}

type TracingStore struct {
	gkvstore.Store

	tracer opentracing.Tracer
}

func NewTracingStore(st gkvstore.Store, tracer opentracing.Tracer) gkvstore.Store {
	return &TracingStore{
		Store:  st,
		tracer: tracer,
	}
}

func (t *TracingStore) startSpan(ctx context.Context, operation string, opts ...opentracing.StartSpanOption) opentracing.Span {
	if pCtx := opentracing.SpanFromContext(ctx); pCtx != nil {
		opts = append(opts, opentracing.ChildOf(pCtx.Context()))
	}
	return t.tracer.StartSpan(operation, opts...)
}

func (t *TracingStore) Create(ctx context.Context, item gkvstore.Item) error {
	span := t.startSpan(ctx, "Create")
	defer span.Finish()

	err := t.Store.Create(context.WithValue(ctx, contextKey{}, span), item)
	if err != nil {
		span.LogFields(tlog.String("error", err.Error()))
	}
	return err
}

func (t *TracingStore) Read(ctx context.Context, item gkvstore.Item) error {
	span := t.startSpan(ctx, "Read")
	defer span.Finish()

	err := t.Store.Read(context.WithValue(ctx, contextKey{}, span), item)
	if err != nil {
		span.LogFields(tlog.String("error", err.Error()))
	}
	return err
}

func (t *TracingStore) Update(ctx context.Context, item gkvstore.Item) error {
	span := t.startSpan(ctx, "Update")
	defer span.Finish()

	err := t.Store.Update(context.WithValue(ctx, contextKey{}, span), item)
	if err != nil {
		span.LogFields(tlog.String("error", err.Error()))
	}
	return err
}

func (t *TracingStore) Delete(ctx context.Context, item gkvstore.Item) error {
	span := t.startSpan(ctx, "Delete")
	defer span.Finish()

	err := t.Store.Delete(context.WithValue(ctx, contextKey{}, span), item)
	if err != nil {
		span.LogFields(tlog.String("error", err.Error()))
	}
	return err
}

func (t *TracingStore) List(ctx context.Context, factory gkvstore.Factory, opts gkvstore.ListOpt) (<-chan *gkvstore.Result, error) {
	span := t.startSpan(ctx, "List")

	res, err := t.Store.List(context.WithValue(ctx, contextKey{}, span), factory, opts)
	if err != nil {
		span.LogFields(tlog.String("error", err.Error()))
		span.Finish()
		return nil, err
	}

	shadow := make(chan *gkvstore.Result)
	go func() {
		defer span.Finish()
		defer close(shadow)

		for {
			select {
			case <-ctx.Done():
				return
			case r, more := <-res:
				if !more {
					return
				}
				if r.Err != nil {
					span.LogFields(tlog.String("error", r.Err.Error()))
				}
				select {
				case <-ctx.Done():
					return
				case shadow <- r:
				}
			}
		}
	}()

	return shadow, nil
}
