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
            "INSERT INTO geolocation VALUES ({0}, {1}, {2}, '{3}', '{4}')".format(line[0], line[1], line[2], city,
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
    chaves = str(frame_data.keys().values).replace('[', '').replace(']', '').replace("'", '').replace(' ', ',')
    for line in frame_data.values:
        cursor.execute(
            "INSERT INTO products VALUES (  '{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}')".format(
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


df = csv_process.import_csv(data.CSV_FILE_PRODUCTS)

process_products(df)

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
