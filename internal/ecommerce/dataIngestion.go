package ecommerce

import (
	"fmt"
	"os"

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

func (o *DataIngestionService) IngestFile(file *os.File) []*SalesRecord {

	records := []*SalesRecord{}
	err := gocsv.UnmarshalFile(file, &records)
	if err != nil {
		panic(err)
	}

	for i, record := range records {
		err := o.store.upsertProduct(record.toProduct())
		if err != nil {
			fmt.Printf("issue inserting record into products table with sku %s, err: %w", record.Sku, err)
		}
		if i == 10 {
			break
		}
	}
	return records
}

func (o *SalesRecord) toProduct() Product {
	return Product{
		Sku:             o.Sku,
		ItemDescription: o.Description,
	}
}
