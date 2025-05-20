# Columbus
[![GoDoc](https://godoc.org/github.com/go-andiamo/columbus?status.svg)](https://pkg.go.dev/github.com/go-andiamo/columbus)
[![Latest Version](https://img.shields.io/github/v/tag/go-andiamo/columbus.svg?sort=semver&style=flat&label=version&color=blue)](https://github.com/go-andiamo/columbus/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/go-andiamo/columbus)](https://goreportcard.com/report/github.com/go-andiamo/columbus)

Columbus is a SQL row mapper that converts rows to a `map[string]any` - saving the need to scan rows into structs and then marshalling those structs as JSON.

Whilst Colombus is primarily focused on reading database rows directly as JSON - it also provides the ability to map rows to structs - without the need to create tedious matching `.Scan()` args.
## Installation
To install columbus, use go get:

    go get github.com/go-andiamo/columbus

To update columbus to the latest version, run:

    go get -u github.com/go-andiamo/columbus

## Examples

<details>
    <summary><strong>Basic example to map all columns from any table</strong></summary>

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

</details><br>

<details>
    <summary><strong>Re-using the same mapper to read all rows or a specific row</strong></summary>

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

</details><br>

<details>
    <summary><strong>Using mappings to add a property</strong></summary>

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

</details><br>

<details>
    <summary><strong>Using row post processor to add a property</strong></summary>

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

</details><br>

<details>
    <summary><strong>Struct mapping</strong></summary>

```go
package main

import (
    "context"
    "database/sql"
    "github.com/go-andiamo/columbus"
)

type Person struct {
    FamilyName string `sql:"family_name"`
    GivenName string  `sql:"given_name"`
}

var PersonMapper = columbus.MustNewStructMapper[Person]("given_name,family_name", columbus.Query("FROM people"))

func GetPeople(ctx context.Context, db *sql.DB, args ...any) ([]Person, error) {
    return PersonMapper.Rows(ctx, db, args)
}
```

</details><br>
