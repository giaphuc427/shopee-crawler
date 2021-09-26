# shopee-crawler
The DAGs is to crawl the information of the shops in the Shopee mall, [here](https://elixirschool.com/en/lessons/basics/basics/). This DAG will return the list of below data into .xlsx file.
- Shop name
- Number of products
- Following
- Chat response rate
- Followers
- Ratings
- Created time

### How to run the repository:
1. Run Airflow via Docker

\>>> docker compose up

2. Click start the 'shopee_etl' Dag.
3. The data returned will be saved in the data folder.
