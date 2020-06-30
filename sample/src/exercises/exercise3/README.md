### Problem 3

Structure of data (ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location)

File format - text file     
                                                                                            
Delimiter - “,” (use regex while splitting split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1), as there are some fields with
 comma and enclosed using double quotes.                                                                                   

Get top 3 crime types based on number of incidents in RESIDENCE area using “Location Description”

Store the result in HDFS path /user/<YOUR_USER_ID>/solutions/solution03/RESIDENCE_AREA_CRIMINAL_TYPE_DATA

Output Fields: Crime Type, Number of Incidents

Output File Format: JSON

Output Delimiter: N/A

Output Compression: No