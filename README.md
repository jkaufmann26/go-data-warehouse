# go-data-warehouse
- This project is to give a breakdown of how I would approach a datawarehous based off Ralph Kimball's: The Data Warehouse Tool Kit

- Setup: Run Docker-compose up in to generate the DB with needed migrations.
- Run Main: to ingest the data csv

- /data/questions.sql to Answer data related questions that business could want. 

- Note ingestion is unoptimized and inserts each record one by one.

- Data was found from: https://www.kaggle.com/datasets/carrie1/ecommerce-data/data

TODO:
- Update Ingest to process in parallel with safe transactions
- Finish Question. Maybe get returned
- Clean up Upsert product to use a hash instead unique keying off a sku. This allows for slow changing dimension
 