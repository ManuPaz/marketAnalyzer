docker exec containerStocks  /usr/bin/mysqldump -u root --password=${PASSWORD} stocks > backup.sql

