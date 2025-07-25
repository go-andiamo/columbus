module postgres

go 1.24.1

require (
	github.com/go-andiamo/columbus v0.0.0
	github.com/lib/pq v1.10.9
	github.com/shopspring/decimal v1.4.0
	github.com/stretchr/testify v1.9.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/go-andiamo/columbus => ../..
