# Columbus
[![GoDoc](https://godoc.org/github.com/go-andiamo/columbus?status.svg)](https://pkg.go.dev/github.com/go-andiamo/columbus)
[![Latest Version](https://img.shields.io/github/v/tag/go-andiamo/columbus.svg?sort=semver&style=flat&label=version&color=blue)](https://github.com/go-andiamo/columbus/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/go-andiamo/columbus)](https://goreportcard.com/report/github.com/go-andiamo/columbus)

Columbus is a SQL row mapper that converts rows to a `map[string]any` - saving the need to scan rows into structs and then marshalling those structs as JSON.

## Installation
To install columbus, use go get:

    go get github.com/go-andiamo/columbus

To update columbus to the latest version, run:

    go get -u github.com/go-andiamo/columbus

## Usage / Examples

A basic example to map all columns from any table...
```go
package main

import (
    "context"
    "database/sql"
    "github.com/go-andiamo/columbus"
)

func ReadRows(ctx context.Context, db *sql.DB, tableName string) ([]map[string]any, error) {
    return columbus.MustNewMapper("*", columbus.Query("FROM "+tableName)).
        Rows(ctx, db, nil)
}
```

Re-using the same mapper to read all rows or a specific row...
```go
package main

import (
    "context"
    "database/sql"
    "github.com/go-andiamo/columbus"
)

var mapper = columbus.MustNewMapper("*", columbus.Query(`FROM people`))

func ReadAll(ctx context.Context, db *sql.DB) ([]map[string]any, error) {
    return mapper.Rows(ctx, db, nil)
}

func ReadById(ctx context.Context, db *sql.DB, id any) (map[string]any, error) {
    return mapper.ExactlyOneRow(ctx, db, []any{id}, columbus.AddClause(`WHERE id = ?`))
}
```

Using mappings to add a property...
```go
package main

import (
    "context"
    "database/sql"
    "github.com/go-andiamo/columbus"
    "strings"
)

func ReadRowsWithInitials(ctx context.Context, db *sql.DB) ([]map[string]any, error) {
    return columbus.MustNewMapper("given_name,family_name",
        columbus.Mappings{
            "family_name": {PostProcess: func(ctx context.Context, sqli columbus.SqlInterface, row map[string]any, value any) (bool, any, error) {
                givenName := row["given_name"].(string)
                familyName := row["family_name"].(string)
                row["initials"] = strings.ToUpper(givenName[:1] + "." + familyName[:1] + ".")
                return false, nil, nil
            }},
        },
        columbus.Query("FROM people")).Rows(ctx, db, nil)
}
```

Using row post processor to add a property...
```go
package main

import (
    "context"
    "database/sql"
    "github.com/go-andiamo/columbus"
    "strings"
)

func ReadRowsWithInitials(ctx context.Context, db *sql.DB) ([]map[string]any, error) {
    return columbus.MustNewMapper("given_name,family_name",
        columbus.RowPostProcessorFunc(func(ctx context.Context, sqli columbus.SqlInterface, row map[string]any) error {
            givenName := row["given_name"].(string)
            familyName := row["family_name"].(string)
            row["initials"] = strings.ToUpper(givenName[:1] + "." + familyName[:1] + ".")
            return nil
        }),
        columbus.Query("FROM people")).Rows(ctx, db, nil)
}
```
