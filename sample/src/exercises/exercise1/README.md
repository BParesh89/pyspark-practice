### Exercise 1

Get monthly count of primary crime type, sorted by month in ascending and number of crimes per type in descending order.

Details - Duration 40 minutes

Data set URL --> https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2

Structure of data (ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location)

File format - text file

Delimiter - “,”

Store the result in HDFS path /user/<YOUR_USER_ID>/solutions/solution01/crimes_by_type_by_month

Output File Format: TEXT

Output Columns: Month in YYYYMM format, crime count, crime type

Output Delimiter: \t (tab delimited)

Output Compression: gzip