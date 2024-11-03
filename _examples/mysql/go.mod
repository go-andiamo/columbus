module mysql

go 1.22.4

require (
	github.com/go-andiamo/columbus v0.0.0
	github.com/go-sql-driver/mysql v1.8.1
	github.com/stretchr/testify v1.9.0
)

replace github.com/go-andiamo/columbus => ../..

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
