cat market_data.sql |docker exec -i containerStocks  /usr/bin/mysql -u root --password=${PASSWORD} market_data
cat stocks.sql |docker exec -i containerStocks  /usr/bin/mysql -u root --password=${PASSWORD} stocks

