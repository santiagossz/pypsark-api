import json
class TopRestaurants():

    def __init__(self):
        
        super().__init__()

    def customer_top_restaurants(self):
        print('----------COUNT OF TOP RESTAURANTS PER CONSUMER----------')
        customer_top10_restaurants=self.spark.sql("""
        with customer_orders as (
            select c.customer_id,c.customer_name,o.merchant_id
            from consumer c
            left join order o 
            on c.customer_id = o.customer_id
            ),
        customer_total_restaurants as (
            select co.customer_id,co.customer_name, r.id restaurant_id, count(*) total_orders
            from customer_orders co
            left join  restaurant r
            on co.merchant_id=r.id
            group by co.customer_id,co.customer_name, r.id
            ),
        restaurant_rank as (
            select *, 
            row_number() over (partition by customer_id  order by total_orders desc) rank 
            from customer_total_restaurants
        )

        select * 
        from restaurant_rank
        where rank<=10

        """ )
        json_response=customer_top10_restaurants.toJSON().collect()

        
        return [json.loads(x) for x in json_response]