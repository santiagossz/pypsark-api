# iFood Data Engineer Test

## 1st Case: API development

This is my proposed API  [iFood Engineer Test](https://github.com/wiflore/ifood-data-engineering-test.git), to serve two endpoints to return the following:


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

run the image to deploy the API container

`docker run -it -p  5000:5000 --name api santiagossz/ifood:api python /home/jovyan/work/main.py --host "localhost"`

run the image to deploy the test cotainer

`docker run -p 8888:8888 -d --name test santiagossz/ifood:api
`

Note: Dpending on your machine resources, the docker image pull may take some time (As it already containts the data warehouse. No need of further
etl process is required)

Before the first request to the API, a spark session will start.
chek the status of the [API](http://localhost:5000/)

 
## endpoints 

- [orders](http://localhost:5000/orders)
- [restaurants](http://localhost:5000/customer-top-restaurants)

You may use an API platform like Insomia/Postman to make the GET requests and get the respective JSON files
Note: Please verify the memory consumption of docker in your machine. As it will kill the spark process the restaurants endpoint if there is no enought memory. 

Testing
Open the following link [localhost:8888](http://localhost:8888/)

in the folder data/ you will see the spark-warehouse (data) 

Open the file work/test/test.ipynb to test the successful api queries
