import os
from flask import Flask
from .pyspark_app.session import PysparkSession
from .endpoints.orders import Orders

app = Flask(__name__)

####check this
@app.before_first_request
def start_spark_sesion():
    global spark
    spark=PysparkSession()


@app.route("/")
def index():
    return {'status':'succesfully connected to data-warehouse'}


@app.route("/orders")
def orders_endpoint():
    
    orders=spark.orders_per_day()

    return orders


@app.route("/customer-top-restaurants")
def restaurants_endpoint():
    
    top_restaurant=spark.customer_top_restaurants()

    return top_restaurant
