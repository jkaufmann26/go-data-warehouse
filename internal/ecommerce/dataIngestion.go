package ecommerce

import (
	"fmt"
	"os"
	"strconv"

	"github.com/gocarina/gocsv"
)

type DataIngestionService struct {
	store Store
}

func NewDataIngestionService(store Store) *DataIngestionService {
	return &DataIngestionService{
		store: store,
	}
}

type SalesRecord struct {
	InvoiceNumber string `csv:"InvoiceNo"`
	Sku           string `csv:"StockCode"`
	Description   string `csv:"Description"`
	Quantity      string `csv:"Quantity"`
	InvoiceDate   string `csv:"InvoiceDate"`
	UnitPrice     string `csv:"UnitPrice"`
	CustomerId    string `csv:"CustomerID"`
	Country       string `csv:"Country"`
}

func (o *DataIngestionService) IngestFile(file *os.File) error {

	records := []*SalesRecord{}
	err := gocsv.UnmarshalFile(file, &records)
	if err != nil {
		panic(err)
	}

	for _, record := range records {
		product, err := o.store.upsertProduct(record.toProduct())
		fmt.Println("Starting another sales insert")
		if err != nil {
			fmt.Println(fmt.Errorf("issue inserting record into products table with sku %s, err: %w", record.Sku, err))
		}

		region, err := o.store.upsertRegion(record.toRegion())
		if err != nil {
			fmt.Println(fmt.Errorf("issue inserting record into region table with sku %s, err: %w", record.Country, err))
		}

		customer, err := o.store.upsertCustomer(record.toCustomer())
		if err != nil {
			fmt.Println(fmt.Errorf("issue inserting record into customer table with sku %s, err: %w", record.CustomerId, err))
		}

		date, err := o.store.getDate(record.InvoiceDate)
		if err != nil {
			fmt.Println(fmt.Errorf("issue getting date from %s, err: %w", record.InvoiceDate, err))
		}

		sale, err := record.toSale(product.Id, region.Id, customer.Id, date.Id)
		if err != nil {
			fmt.Println(fmt.Errorf("issue converting to sales record, err: %w", err))
		}

		sale, err = o.store.insertSalesRecord(sale)
		if err != nil {
			fmt.Println(fmt.Errorf("issue inserting sales record, err: %w", err))
		}
		fmt.Printf("record inserted %s\n", sale.Id)
	}
	return nil
}

func (o *SalesRecord) toProduct() Product {
	return Product{
		Sku:             o.Sku,
		ItemDescription: o.Description,
	}
}

func (o *SalesRecord) toRegion() Region {
	return Region{
		RegionName: o.Country,
	}
}

func (o *SalesRecord) toCustomer() Customer {
	return Customer{
		Id: o.CustomerId,
	}
}

func (o *SalesRecord) toSale(productKey string, regionKey string, customerKey string, dateKey string) (Sale, error) {
	quantity, err := strconv.Atoi(o.Quantity)
	if err != nil {
		return Sale{}, fmt.Errorf("an error occurred while parsing quantity: %w", err)
	}
	unitPrice, err := strconv.ParseFloat(o.UnitPrice, 32)
	if err != nil {
		return Sale{}, fmt.Errorf("an error occurred while parsing quantity: %w", err)
	}
	return Sale{
		InvoiceId:     o.InvoiceNumber,
		ProductKey:    productKey,
		DateKey:       dateKey,
		CustomerKey:   customerKey,
		RegionKey:     regionKey,
		SalesQuantity: quantity,
		UnitPrice:     float32(unitPrice),
	}, nil
}
