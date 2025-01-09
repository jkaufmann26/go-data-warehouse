package ecommerce

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type Store struct {
	db *sqlx.DB
}

func NewDataStore(db *sql.DB) *Store {
	return &Store{
		db: sqlx.NewDb(db, "postgres"),
	}
}

type Product struct {
	Id              string `db:"id"`
	Sku             string `db:"sku"`
	ItemDescription string `db:"item_description"`
}

type Date struct {
	Id   string
	Date time.Time
}

type Sale struct {
	Id            string
	ProductKey    string
	DateKey       string
	CustomerKey   string
	RegionKey     string
	SalesQuantity int
	UnitPrice     float32
}

type Region struct {
	Id         string
	RegionName string
}

type Customer struct {
	Id         string
	customerId string
}

func (o *Store) upsertProduct(product Product) error {
	product.Id = uuid.NewString()
	_, err := o.db.NamedExec(
		`INSERT INTO products (
		id,
		sku,
		item_description
	) VALUES (
		:id,
		:sku,
		:item_description
	) `,
		product)

	if err != nil {
		return fmt.Errorf("error inserting products: %w", err)
	}
	return nil
}
