import csv_process
import database
import data

# Create table
database.create_table(data.SQL_TABLE_GEOLOCATION, 'geolocation')
database.create_table(data.SQL_TABLE_PEOPLE, 'people')
database.create_table(data.SQL_TABLE_ORDERS, 'orders')
database.create_table(data.SQL_TABLE_ORDER_PAY, 'order_payments')
database.create_table(data.SQL_TABLE_PRODUCTS, 'products')
database.create_table(data.SQL_TABLE_ORDER_ITEM, 'order_items')




#df = csv_process.import_csv(CSV_FILE_GEOLOCATION)


#conecction1 = database.con_database()
#cur = conecction1.cursor()


#for line in df.values:
#    city = "{0}".format(line[3]).replace("'", " ")
#    state = "{0}".format(line[4])
#    city = city.upper()
#    state = state.upper()

   # cur.execute(
   #     "INSERT INTO geolocation VALUES ({0}, {1}, {2}, '{3}', '{4}')".format(line[0], line[1], line[2], city, state)
   # )
   # conecction1.commit()

#cur.close()
#conecction1.close()

