# gkvstore [![Go](https://github.com/plexsysio/gkvstore/workflows/Go/badge.svg)](https://github.com/plexsysio/gkvstore/actions) [![Go Reference](https://pkg.go.dev/badge/github.com/plexsysio/gkvstore.svg)](https://pkg.go.dev/github.com/plexsysio/gkvstore) [![Coverage Status](https://coveralls.io/repos/github/plexsysio/gkvstore/badge.svg?branch=main)](https://coveralls.io/github/plexsysio/gkvstore?branch=main)
Generic key-value store

gkvstore is a generic KV Store interface which provides an easier
interface to access underlying database. It is mainly used to abstract
the database used underneath so we can have a uniform API to use for clients

Different DB adapters can be built for the Store. This allows decoupling DB code
from specific implementations and ability to change DBs on the fly.


## Install
`gkvstore` works like a regular Go module:

```
> go get github.com/plexsysio/gkvstore
```

## Usage
```go
import "github.com/plexsysio/gkvstore"
```
