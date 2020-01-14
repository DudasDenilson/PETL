import psycopg2


def con_database():
    con = None
    try:
        con = psycopg2.connect(host='localhost', database='dwteste', user='postgres', password='postgres')
        return con
    except Exception as e:
        print('Problem to connect ' + str(e))

    return con


# sql = 'create table cidade (id serial primary key, nome varchar(100), uf varchar(2))'
# cur.execute(sql)


def cursor_database(connection):
    con = connection
    cur = con.cursor()
    return cur


def execute_sql_postgres(string_sql, cursor_connection):
    execu = cursor_connection.execute(string_sql)
    print(execu)

# sql = "insert into cidade (nome, uf) values ('São pedro','SP')"
# cur.execute(sql)

# con.commit()
# cur.execute('select * from cidade')
# recset = cur.fetchall()
# for rec in recset:
#    print (rec)
# con.close()


def create_table(sql_create, table_name):
    try:
        con = con_database()
        cursor = con.cursor()
        cursor.execute(sql_create)
        con.commit()
        con.close()
        print('Table ' + table_name + ' created.')
    except Exception as e:
        print('Failed to create table ' + table_name + ' ' + str(e))
        drop_table(table_name)
        create_table(sql_create, table_name)



def drop_table(table_name):
    con = con_database()
    cursor = con.cursor()
    SQL_DROP = 'drop table '
    try:
        cursor.execute(SQL_DROP + table_name + ' cascade;')
        con.commit()
        con.close()
        print('Table ' + table_name + ' dropped.')
    except Exception as e:
        print('Failed to drop table ' + table_name + ' ' + str(e))
