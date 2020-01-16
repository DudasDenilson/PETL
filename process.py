import csv_process
import database
import data
import datetime

# Create table
database.create_table(data.SQL_TABLE_GEOLOCATION, 'geolocation')
database.create_table(data.SQL_TABLE_PEOPLE, 'people')
database.create_table(data.SQL_TABLE_ORDERS, 'orders')
database.create_table(data.SQL_TABLE_ORDER_PAY, 'order_payments')
database.create_table(data.SQL_TABLE_PRODUCTS, 'products')
database.create_table(data.SQL_TABLE_ORDER_ITEM, 'order_items')


def process_geolocation(frame_data):
    print('Started treatment process at ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    con = database.con_database()
    cursor = con.cursor()
    for line in frame_data.values:
        city = "{0}".format(line[3]).replace("'", " ").upper()
        state = "{0}".format(line[4]).upper()
        cursor.execute(
            """INSERT INTO geolocation (zip_code_geolocation,geolocation_lat,geolocation_long,geolocation_city,
            geolocation_state) VALUES ({0}, {1}, {2}, '{3}', '{4}')""".format(line[0], line[1], line[2], city,
                                                                              state)
        )
        con.commit()
    print('Done treatment process at ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    cursor.close()
    con.close()


def process_products(frame_data):
    print('Started treatment process at ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    con = database.con_database()
    cursor = con.cursor()
    #  chaves = str(frame_data.keys().values).replace('[', '').replace(']', '').replace("'", '').replace(' ', ',')
    for line in frame_data.values:
        cursor.execute(
            """INSERT INTO products (product_id,product_category_name,product_name_lenght,product_description_lenght, 
            product_photos_qty,product_weight_g,product_length_cm,product_height_cm,product_width_cm) VALUES (  '{0}',
             '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', 
            '{7}', '{8}')""".format(
                line[0], line[1],
                line[2], line[3],
                line[4], line[5],
                line[6], line[7], line[8]
            )
        )
        # print(str(line).replace('[', '').replace(']', '').replace(' ', ','))
        # dados = str(line).replace('[', '').replace(']', '').replace(' ', ',')
        # cursor.execute(
        #    "insert into products({0}) values ({1})".format(chaves, dados)
        # )
        # print(dados)
        con.commit()
    print('Done treatment process at ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    cursor.close()
    con.close()


def process_people(frame_customer, frame_sellers):
    print('Started treatment process at ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    con = database.con_database()
    cursor = con.cursor()
    for line in frame_customer.values:
        # print(line)
        cursor.execute(
            """INSERT INTO people(people_id, people_unique_id, people_zip_code_prefix, people_classification) 
            VALUES ('{0}', '{1}', '{2}', '{3}')""".format(line[0], line[1], line[2], 'CUSTOMER')
        )

        con.commit()

    for line in frame_sellers.values:
        # print(line)
        cursor.execute(
            """INSERT INTO people(people_id,  people_zip_code_prefix, people_classification) 
            VALUES ('{0}', '{1}', '{2}')""".format(line[0], line[1], 'SELLER')
        )
        con.commit()

    print('Done treatment process at ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    cursor.close()
    con.close()


def process_order(frame_data):
    print('Started treatment process at ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    con = database.con_database()
    frame_data = frame_data.fillna('')
    cursor = con.cursor()
    for line in frame_data.values:
        #print(line)
        cursor.execute(
            '''INSERT INTO orders (order_id,customer_id,order_status, order_purchase_timestamp, order_approved_at, 
            order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date  ) 
            VALUES ('{0}', '{1}', '{2}',TO_TIMESTAMP('{3}','yyyy-mm-dd hh24:mi:ss'),
                                        TO_TIMESTAMP('{4}','yyyy-mm-dd hh24:mi:ss'),
                                        TO_TIMESTAMP('{5}','yyyy-mm-dd hh24:mi:ss'),
                                        TO_TIMESTAMP('{6}','yyyy-mm-dd hh24:mi:ss'),
                                        TO_TIMESTAMP('{7}','yyyy-mm-dd hh24:mi:ss'))'''.format(line[0], line[1], line[2],
                                                                 (line[3]),
                                                                 (line[4]),
                                                                 (line[5]),
                                                                 (line[6]),
                                                                 (line[7]))
        )
        con.commit()
    print('Done treatment process at ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    cursor.close()
    con.close()


def process_order_pay(frame_data):
    print('Started treatment process at ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    con = database.con_database()
    cursor = con.cursor()
    for line in frame_data.values:
        cursor.execute(
            """INSERT INTO order_payments(order_id ,payment_sequential,payment_type,payment_installments,payment_value) 
            VALUES ('{0}', '{1}', '{2}', '{3}', '{4}')""".format(line[0], line[1], line[2], line[3], line[4])
        )
        con.commit()
    print('Done treatment process at ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    cursor.close()
    con.close()


def process_order_item(frame_data):
    print('Started treatment process at ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    con = database.con_database()
    cursor = con.cursor()
    for line in frame_data.values:
        cursor.execute(
            """INSERT INTO order_items(order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value) 
            VALUES ('{0}', '{1}', '{2}', '{3}', '{4}','{5}','{6}')""".format(line[0], line[1], line[2], line[3], line[4], line[5], line[6])
        )
        con.commit()
    print('Done treatment process at ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    cursor.close()
    con.close()


dfs = csv_process.import_csv(data.CSV_FILE_SELLERS)
dfc = csv_process.import_csv(data.CSV_FILE_CUSTOMER)
process_people(dfc, dfs)

df = csv_process.import_csv(data.CSV_FILE_ORDERS)
process_order(df)

dfp = csv_process.import_csv(data.CSV_FILE_ORDER_PAY)
process_order_pay(dfp)

df = csv_process.import_csv(data.CSV_FILE_PRODUCTS)
process_products(df)

dfi = csv_process.import_csv(data.CSV_FILE_ORDER_ITEM)
process_order_item(dfi)

dfg = csv_process.import_csv(data.CSV_FILE_GEOLOCATION)
process_geolocation(dfg)

# conecction1 = database.con_database()
# cur = conecction1.cursor()


# for line in df.values:
#    city = "{0}".format(line[3]).replace("'", " ")
#    state = "{0}".format(line[4])
#    city = city.upper()
#    state = state.upper()

# cur.execute(
#     "INSERT INTO geolocation VALUES ({0}, {1}, {2}, '{3}', '{4}')".format(line[0], line[1], line[2], city, state)
# )
# conecction1.commit()

# cur.close()
# conecction1.close()
