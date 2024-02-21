

set MYSQL_HOST=AF53926.ap-southeast-1
set MYSQL_USERNAME=Demo
set MYSQL_PASSWORD=Demo@12345
set MYSQL_DATABASE=TEST


:: To run the pipeline for a customer id
:; python pipeline_mysql.py -p google_ads.yaml -c 6486894436

:: To run the pipeline for multiple customer id (active status in the google sheet)
python pipeline.py -p google_ads.yaml