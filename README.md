# iFood Data Engineer Test

## 1st Case: API development

This is my proposed API  [iFood Engineer Test](https://github.com/ifood/ifood-data-architect-test), to serve two endpoints to return the following:


  * Count of orders per day for each city and state in our database
  * Top 10 restaurants per customer



## Solution

Frameworks: 
- The data analysis was done using Apache Spark Python interface **PySpark** using spark.sql to query the data from the warehouse
- The enpoints were serve using Flask 


### Requirements

* `docker >= 19.03.9`

## Steps to Run

pull the docker image from docker hub

`docker pull santiagossz/ifood:api`

run the image 

`docker run -p 8888:8888 -d --name api santiagossz/ifood:api
`

execute the python script to complete the etl

`docker exec -it api python /home/jovyan/work/main.py`

Note: Dpending on your machine resources, the docker image pull may take some time (As the data warehouse was included - for not having to download & store 
the data, as it will take longer )

Before the first request to the API, a spark session will start.

 
## endpoints 

- orders (http://localhost:8080/orders)
- restaurants (http://localhost:8080/customer-top-restaurants)

You may use an API platform like Insomia/Postman to make the GET requests and get the respective JSON files

Testing
Open the following link localhost:8888 (http://localhost:8888/)

in the folder data/ you will see the spark-warehouse (data) & catalog (metadata)

Open the file work/test/test.ipynb to test the successful api queries
