module mysql

go 1.22.4

require github.com/go-andiamo/columbus v0.0.0

replace github.com/go-andiamo/columbus => ../..

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/go-sql-driver/mysql v1.8.1 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
)
