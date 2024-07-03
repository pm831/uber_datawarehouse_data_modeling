# 📝 Building a Uber Data Model for Data Warehousing

A data warehouse is an essential component of Uber's data infrastructure, enabling it to harness the power of its vast data assets. By providing a centralized, integrated, and scalable platform for data storage and analysis, the data warehouse helps Uber enhance its operational efficiency, customer satisfaction, and overall business performance.

### Let's dive into the Project to explore more!! 

https://github.com/pm831/uber_datawarehouse

### Data Curation Process (Python):

uber_data pipeline.ipynb

##### Importing Necessary Libraries 

Importing the necessary libraries for data manipulation (pandas), making HTTP requests (requests), interacting with the file system (os), and geocoding (geopy). json_normalize from pandas to flatten JSON data.

##### Loading and Preprocessing Data 

Reads the Uber data CSV file, converts date columns to datetime objects, removes duplicates, and assigns a unique trip ID to each record.

##### Creating the Dimension Tables 

A DateTime dimension table with pickup and dropoff times, and various time components like hour, day, month, year, and weekday. It assigns unique IDs to each datetime record. In addition, other dimension tables for passenger count, trip distance, and rate codes are created. The rate codes are mapped to human-readable names. A payment type dimension table is created mapping numeric codes to descriptive names.

##### Geocoding Pickup and Dropoff Locations Dimension Tables (Latitude and Longitude) 

Defines functions for reverse geocoding using the OpenCage API. It includes error handling for common geocoding issues. Then apply the reverse geocoding function to the pickup and dropoff locations, expanding the results into new columns for addresses and detailed location information. The json_normalize function flattens the nested JSON data.

##### Creating the Fact Table 

Creates the fact table by merging the main data frame with all the dimension tables created earlier. The resulting table includes various attributes like trip ID, vendor ID, datetime, passenger count, trip distance, rate code, pickup and dropoff locations, payment type, and fare details. The script effectively organizes and enriches the Uber dataset, making it more suitable for analysis and reporting.

##### Fact Table

The fact_table contains the primary quantitative data about each Uber trip. 
It includes the following measures and foreign keys:

##### Measures: 
* fare_amount
* extra, mta_tax
* tip_amount
* tolls_amount
* improvement_surcharge
* total_amount

##### Foreign Keys: 
* datetime_id
* passenger_count_id
* trip_distance_id
* rate_code_id
* pickup_location_id
* dropoff_location_id
* payment_type_id

##### Dimension Tables

##### Datetime Dimension: datetime_dim
This table contains attributes related to the pickup and dropoff times, such as hour, day, month, year, and weekday.
 
##### Passenger Count Dimension: passenger_count_dim 
This table contains attributes related to the number of passengers.

##### Trip Distance Dimension: trip_distance_dim 
This table contains attributes related to the distance of the trip.

##### Rate Code Dimension: rate_code_dim 
This table contains attributes related to the rate codes, including a descriptive name for each rate code.

##### Pickup Location Dimension: pickup_location_dim
This table contains attributes related to the pickup location, including latitude, longitude, address, and additional location details.

##### Dropoff Location Dimension: dropoff_location_dim 
This table contains attributes related to the dropoff location, including latitude, longitude, address, and additional location details.

##### Payment Type Dimension: payment_type_dim 
This table contains attributes related to the payment types, including a descriptive name for each payment type.

Below, once the tables are uploaded to SQLDB, we can join the tables and extract the columns we want for business purposes.

Essentially, this practice of subscribing to an API that contains information, automating that job and data model in Python, writing out the tables in a SQLDB, and performing various queries for data insights and analysis is the most important foundation any business can take to always be tuned in on the latest insights and decisions!!

Later, I'll take about the latest best practices on how to use data orchestration methods such as Apache Airflow to schedule and optimize the data workloads and jobs! Stay tuned!
