#Sql Scripts
SQL_TABLE_ORDERS = """
create table orders (
order_id varchar(50),
customer_id varchar(50) references people(people_id),
order_status varchar(30),
order_purchase_timestamp timestamp,
order_approved_at timestamp,
order_delivered_carrier_date timestamp ,
order_delivered_customer_date timestamp ,
order_estimated_delivery_date timestamp ,
primary key(order_id));
"""

SQL_TABLE_GEOLOCATION = """create table geolocation (
                                    zip_code_geolocation numeric , 
                                    geolocation_lat numeric, 
                                    geolocation_long numeric, 
                                    geolocation_city varchar(100), 
                                    geolocation_state varchar(2) );
                        """

SQL_TABLE_ORDER_PAY = """
create table order_payments(
order_id varchar(50),
payment_sequential numeric,
payment_type varchar(20),
payment_installments numeric,
payment_value float,
foreign key(order_id) references orders(order_id)
);
"""
SQL_TABLE_ORDER_ITEM = """
create table order_items(
order_id varchar(50) ,
order_item_id numeric,
product_id varchar(50) references products(product_id),
seller_id varchar(50) references people(people_id),
shipping_limit_date timestamp ,
price float,
freight_value float,
foreign key(order_id) references orders(order_id)
);
"""

SQL_TABLE_PRODUCTS = """
create table products (
product_id varchar(50) primary key,
product_category_name  varchar(80),
product_name_lenght numeric,
product_description_lenght numeric(10),
product_photos_qty numeric(10),
product_weight_g numeric(10),
product_length_cm numeric(10),
product_height_cm numeric(10),
product_width_cm numeric(10)

);
"""


SQL_TABLE_PEOPLE = """
create table people (
people_id varchar(50) primary key,
people_unique_id varchar(50),
people_zip_code_prefix numeric ,
people_classification varchar(30)
)

"""










# Csv Location
CSV_FILE_GEOLOCATION = 'https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_geolocation_dataset.csv'
CSV_FILE_ORDERS =  'https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_orders_dataset.csv'
CSV_FILE_ORDER_PAY =  'https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_order_payments_dataset.csv'
CSV_FILE_ORDER_ITEM = 'https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_order_items_dataset.csv'
CSV_FILE_PRODUCTS = 'https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_products_dataset.csv'
CSV_FILE_SELLERS = 'https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_sellers_dataset.csv'
CSV_FILE_CUSTOMER = 'https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets/olist_customers_dataset.csv'

