from flask import Flask, jsonify
from .pyspark_app.session import PysparkSession

app = Flask(__name__)

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

    return jsonify(orders)


@app.route("/customer-top-restaurants")
def restaurants_endpoint():
    
    top_restaurant=spark.customer_top_restaurants()

    return jsonify(top_restaurant)
