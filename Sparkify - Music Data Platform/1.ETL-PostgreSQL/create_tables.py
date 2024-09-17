
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def create_database():

    # connect to the default database 

    conn = psycopg2.connect("host=127.0.0.1 dbname = yuforstudydb user=yuli password = admin")
    conn.set_session(autocommit=True ) #每个 SQL 语句在执行后会立即被提交到数据库，而不需要显式调用 commit() 方法。
    cur = conn.cursor() #控制结构，使得我们能够遍历结果集中的记录。可以将其想象为数据库结果集的指针。Pointer.

    #create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf-8'")

    #close connection to default database 
    conn.close()

    #connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=yuli password = admin ")
    cur = conn.cursor()

    return cur,conn


def drop_tables(cur,conn):
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur,conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():

    cur, conn = create_database()
    drop_tables(cur,conn)
    print("Table dropped successfully!!")

    create_tables(cur, conn)
    print("Table created successfully!!")

    conn.close()

    

        

if __name__ == "__main__":
    main()