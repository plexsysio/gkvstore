package prefixstore_test

import (
	"context"
	"testing"

	"github.com/plexsysio/gkvstore"
	"github.com/plexsysio/gkvstore/autoencoding"
	"github.com/plexsysio/gkvstore/inmem"
	prefixstore "github.com/plexsysio/gkvstore/prefix"
	syncstore "github.com/plexsysio/gkvstore/sync"
)

type user struct {
	Id   string
	Name string
	Age  int64
}

type product struct {
	Id    string
	Name  string
	Price int64
}

func TestCRUDL(t *testing.T) {
	st1 := inmem.New()
	st2 := syncstore.New(inmem.New())
	pfxStore := prefixstore.New(
		prefixstore.Mount{
			Prefix: "user",
			Store:  st1,
		},
		prefixstore.Mount{
			Prefix: "product",
			Store:  st2,
		},
	)

	t.Run("Create", func(t *testing.T) {
		err := pfxStore.Create(context.TODO(), autoencoding.MustNew(&user{
			Name: "user1",
			Age:  20,
		}))
		if err != nil {
			t.Fatal(err)
		}
		err = pfxStore.Create(context.TODO(), autoencoding.MustNew(&product{
			Name:  "product1",
			Price: 100,
		}))
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("Read", func(t *testing.T) {
		u1 := &user{Id: "1"}
		err := pfxStore.Read(context.TODO(), autoencoding.MustNew(u1))
		if err != nil {
			t.Fatal(err)
		}
		if u1.Name != "user1" || u1.Age != 20 {
			t.Fatal("incorrect value read")
		}
		p1 := &product{Id: "1"}
		err = pfxStore.Read(context.TODO(), autoencoding.MustNew(p1))
		if err != nil {
			t.Fatal(err)
		}
		if p1.Name != "product1" || p1.Price != 100 {
			t.Fatal("incorrect value read")
		}
	})
	t.Run("Read individual stores", func(t *testing.T) {
		u1 := &user{Id: "1"}
		err := st1.Read(context.TODO(), autoencoding.MustNew(u1))
		if err != nil {
			t.Fatal(err)
		}
		if u1.Name != "user1" || u1.Age != 20 {
			t.Fatal("incorrect value read")
		}
		err = st2.Read(context.TODO(), autoencoding.MustNew(u1))
		if err == nil {
			t.Fatal("expected error from second store for user")
		}
		p1 := &product{Id: "1"}
		err = st1.Read(context.TODO(), autoencoding.MustNew(p1))
		if err == nil {
			t.Fatal("expected error from first store for product")
		}
		err = st2.Read(context.TODO(), autoencoding.MustNew(p1))
		if err != nil {
			t.Fatal(err)
		}
		if p1.Name != "product1" || p1.Price != 100 {
			t.Fatal("read incorrect product details")
		}
	})
	t.Run("Update", func(t *testing.T) {
		err := pfxStore.Update(context.TODO(), autoencoding.MustNew(&user{
			Id:   "1",
			Name: "user1",
			Age:  30,
		}))
		if err != nil {
			t.Fatal(err)
		}
		err = pfxStore.Update(context.TODO(), autoencoding.MustNew(&product{
			Id:    "1",
			Name:  "product1",
			Price: 200,
		}))
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("Read individual stores after update", func(t *testing.T) {
		u1 := &user{Id: "1"}
		err := st1.Read(context.TODO(), autoencoding.MustNew(u1))
		if err != nil {
			t.Fatal(err)
		}
		if u1.Name != "user1" || u1.Age != 30 {
			t.Fatal("incorrect value read")
		}
		err = st2.Read(context.TODO(), autoencoding.MustNew(u1))
		if err == nil {
			t.Fatal("expected error from second store for user")
		}
		p1 := &product{Id: "1"}
		err = st1.Read(context.TODO(), autoencoding.MustNew(p1))
		if err == nil {
			t.Fatal("expected error from first store for product")
		}
		err = st2.Read(context.TODO(), autoencoding.MustNew(p1))
		if err != nil {
			t.Fatal(err)
		}
		if p1.Name != "product1" || p1.Price != 200 {
			t.Fatal("read incorrect product details")
		}
	})
	t.Run("List", func(t *testing.T) {
		res, err := pfxStore.List(context.TODO(), func() gkvstore.Item {
			return autoencoding.MustNew(&user{})
		}, gkvstore.ListOpt{Limit: 5})
		if err != nil {
			t.Fatal(err)
		}
		countUsr := 0
		for v := range res {
			countUsr++
			u, ok := v.Val.(autoencoding.ObjectGetter).Get().(*user)
			if !ok {
				t.Fatal("incorrect obj in list response")
			}
			if u.Name != "user1" || u.Age != 30 {
				t.Fatal("incorrect value returned on List")
			}
		}
		if countUsr != 1 {
			t.Fatal("incorrect no of items returned in List")
		}
		res, err = pfxStore.List(context.TODO(), func() gkvstore.Item {
			return autoencoding.MustNew(&product{})
		}, gkvstore.ListOpt{Limit: 5})
		if err != nil {
			t.Fatal(err)
		}
		countProd := 0
		for v := range res {
			countProd++
			p, ok := v.Val.(autoencoding.ObjectGetter).Get().(*product)
			if !ok {
				t.Fatal("incorrect obj in list response")
			}
			if p.Name != "product1" || p.Price != 200 {
				t.Fatal("incorrect value returned on List")
			}
		}
		if countProd != 1 {
			t.Fatal("incorrect no of items returned in List")
		}
	})
	t.Run("Delete", func(t *testing.T) {
		err := pfxStore.Delete(context.TODO(), autoencoding.MustNew(&user{
			Id: "1",
		}))
		if err != nil {
			t.Fatal(err)
		}
		err = pfxStore.Delete(context.TODO(), autoencoding.MustNew(&product{
			Id: "1",
		}))
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("Read individual stores after delete", func(t *testing.T) {
		u1 := &user{Id: "1"}
		err := st1.Read(context.TODO(), autoencoding.MustNew(u1))
		if err == nil {
			t.Fatal("expected error after delete")
		}
		err = st2.Read(context.TODO(), autoencoding.MustNew(u1))
		if err == nil {
			t.Fatal("expected error from second store for user")
		}
		p1 := &product{Id: "1"}
		err = st1.Read(context.TODO(), autoencoding.MustNew(p1))
		if err == nil {
			t.Fatal("expected error from first store for product")
		}
		err = st2.Read(context.TODO(), autoencoding.MustNew(p1))
		if err == nil {
			t.Fatal("expected error after delete")
		}
	})
}
