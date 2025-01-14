package ecommerce

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
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
	ReceiptId     string  `db:"receipt_id"`
	ProductId     string  `db:"product_id"`
	DateId        string  `db:"date_id"`
	CustomerId    string  `db:"customer_id"`
	RegionId      string  `db:"region_id"`
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
	product.Id = HashValues(product.Sku, product.ItemDescription)
	rows, err := o.db.NamedQuery(
		`INSERT INTO products (
		id,
		sku,
		item_description
	) VALUES (
		:id,
		:sku,
		:item_description
	)  ON CONFLICT (id) DO NOTHING`,
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
	sale.ReceiptId = sale.InvoiceId + sale.ProductId
	rows, err := o.db.NamedQuery(
		`INSERT INTO sales (
		id,
		invoice_id,
		receipt_id,
		product_id,
		date_id,
		customer_id,
		region_id,
		sales_quantity,
		unit_price
		) VALUES (
		:id,
		:invoice_id,
		:receipt_id,
		:product_id,
		:date_id,
		:customer_id,
		:region_id,
		:sales_quantity,
		:unit_price
	) ON CONFLICT (receipt_id) DO UPDATE
	SET updated_at = NOW()`,
		sale)

	if err != nil {
		return Sale{}, fmt.Errorf("error sales record: %w", err)
	}
	rows.Close()
	return sale, nil
}

func Hash(s string) string {
	h := sha256.New()
	h.Write([]byte(s))

	encodedHash := hex.EncodeToString(h.Sum(nil))

	uuid, err := uuid.FromBytes([]byte(encodedHash[0:16]))
	if err != nil {
		panic(err)
	}

	return uuid.String()
}

func HashValues(values ...any) string {
	s := ""
	for _, v := range values {
		s += fmt.Sprintf("%v", v)
	}
	return Hash(s)
}
