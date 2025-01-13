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
	Id               string    `db:"id"`
	Date             time.Time `db:"date_val"`
	Description      string    `db:"full_day_description"`
	DayOfWeek        string    `db:"day_of_week"`
	CalendarMonth    string    `db:"calendar_month"`
	CalendarYear     string    `db:"calendar_year"`
	FiscalMonth      string    `db:"fiscal_month"`
	HolidayIndicator bool      `db:"holiday_indicator"`
	WeekDayIndicator bool      `db:"weekday_indicator"`
}

type Sale struct {
	Id            string  `db:"id"`
	InvoiceId     string  `db:"invoice_id"`
	ReceiptKey    string  `db:"receipt_key"`
	ProductKey    string  `db:"product_key"`
	DateKey       string  `db:"date_key"`
	CustomerKey   string  `db:"customer_key"`
	RegionKey     string  `db:"region_key"`
	SalesQuantity int     `db:"sales_quantity"`
	UnitPrice     float32 `db:"unit_price"`
}

type Region struct {
	Id         string `db:"id"`
	RegionName string `db:"region_name"`
}

type Customer struct {
	Id string `db:"id"`
}

func (o *Store) upsertProduct(product Product) (Product, error) {
	product.Id = uuid.NewString()
	rows, err := o.db.NamedQuery(
		`INSERT INTO products (
		id,
		sku,
		item_description
	) VALUES (
		:id,
		:sku,
		:item_description
	)  ON CONFLICT (sku) DO UPDATE
	SET updated_at = NOW(), sku=:sku, item_description=:item_description`,
		product)

	if err != nil {
		return Product{}, fmt.Errorf("error inserting products: %w", err)
	}
	rows.Close()

	return product, nil
}

func (o *Store) upsertCustomer(customer Customer) (Customer, error) {
	rows, err := o.db.NamedQuery(
		`INSERT INTO customers (
		id
		) VALUES (
		:id
	) ON CONFLICT (id) DO UPDATE
	SET updated_at = NOW()`,
		customer)

	if err != nil {
		return Customer{}, fmt.Errorf("error inserting customer: %w", err)
	}
	rows.Close()
	return customer, nil
}

func (o *Store) upsertRegion(region Region) (Region, error) {
	region.Id = uuid.NewString()
	rows, err := o.db.NamedQuery(
		`INSERT INTO regions (
		id,
		region_name
		) VALUES (
		:id,
		:region_name) ON CONFLICT (region_name) DO NOTHING`,
		region)

	if err != nil {
		return Region{}, fmt.Errorf("error inserting region: %w", err)
	}
	rows.Close()
	return region, nil
}

func (o *Store) getDate(date string) (Date, error) {
	format := "1/2/2006 15:04"
	var dateValue time.Time
	dateValue, err := time.Parse(format, date)
	if err != nil {
		return Date{}, err
	}
	var output Date
	err = o.db.Get(&output, `SELECT * FROM date_dimension 
	WHERE  (date_part('year', date_val) = $1
	AND    date_part('month', date_val) = $2
	AND    date_part('day', date_val) = $3)`, dateValue.Year(), dateValue.Month(), dateValue.Day())
	if err != nil {
		return Date{}, err
	}
	return output, nil
}

func (o *Store) insertSalesRecord(sale Sale) (Sale, error) {
	sale.Id = uuid.NewString()
	sale.ReceiptKey = sale.InvoiceId + sale.ProductKey
	rows, err := o.db.NamedQuery(
		`INSERT INTO sales (
		id,
		invoice_id,
		receipt_key,
		product_key,
		date_key,
		customer_key,
		region_key,
		sales_quantity,
		unit_price
		) VALUES (
		:id,
		:invoice_id,
		:receipt_key,
		:product_key,
		:date_key,
		:customer_key,
		:region_key,
		:sales_quantity,
		:unit_price
	) ON CONFLICT (receipt_key) DO UPDATE
	SET updated_at = NOW()`,
		sale)

	if err != nil {
		return Sale{}, fmt.Errorf("error sales record: %w", err)
	}
	rows.Close()
	return sale, nil
}
