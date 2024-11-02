module postgres

go 1.22.4

require github.com/go-andiamo/columbus v0.0.0

require (
	github.com/lib/pq v1.10.9 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
)

replace github.com/go-andiamo/columbus => ../..
