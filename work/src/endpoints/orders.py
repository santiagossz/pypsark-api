import json
class Orders():

    def __init__(self):
        super().__init__()

    def orders_per_day(self):
        print('----------COUNT OF ORDERS PER DAY PER CITY/STATE----------')
        orders=self.spark.sql("""
        select  delivery_address_state,delivery_address_city, date(order_created_at), count(*) as num_orders
        from order 
        group by delivery_address_state,delivery_address_city,date(order_created_at) 
        order by delivery_address_state,delivery_address_city,date(order_created_at)
        """)
        json_response=orders.toJSON().collect()

        
        return [json.loads(x) for x in json_response]