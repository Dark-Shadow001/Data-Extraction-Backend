
set MYSQL_HOST=AF53926.ap-southeast-1
set MYSQL_USERNAME=Demo
set MYSQL_PASSWORD=Demo@12345
set MYSQL_DATABASE=TEST

:: This will upsert the google sheet data into mysql table 'customers'
python gsheet_to_mysql.py