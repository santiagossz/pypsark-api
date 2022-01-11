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

`docker run -it  -p 8080:5000 santiagossz/ifood:api  
`


Note: Dpending on your machine resources, the docker image pull may take some time (As the data warehouse was included - for not having to download & store 
the data, as it will take longer )

Before the first request to the API, a spark session will start.

 
## endpoints 

- orders (http://localhost:8080/orders)
- restaurants (http://localhost:8080/customer-top-restaurants)

You may use an API platform like Insomia/Postman to make the GET requests and get the respective JSON files
