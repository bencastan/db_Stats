#!/usr/bin/python
import psycopg2
from config import config
import csv
import datetime

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read conection parameters
        params = config()

        # connect to the postgreSQL server
        print('Connecting to the PostgresSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgresSQL server version
        db_version = cur.fetchone()
        print(db_version)

        # close the communication with the PostGreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def get_publish():
    """ Query 7 day stats for the publish table"""
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("SELECT \
        ROUND(avg(EXTRACT(EPOCH FROM (finish::timestamp - start::timestamp))/60)::numeric,2) as average_Minutes, \
        ROUND(min(EXTRACT(EPOCH FROM (finish::timestamp - start::timestamp))/60)::numeric,2) as min_Minutes, \
        ROUND(max(EXTRACT(EPOCH FROM (finish::timestamp - start::timestamp))/60)::numeric,2) as max_Minutes \
    FROM \
        publish p \
    WHERE \
        finish > (CURRENT_TIMESTAMP - INTERVAL '7 day') and p.preview = 'f' \
    LIMIT 1000;")
        rows = cur.fetchall()
        #print("The number of rows is: ", cur.rowcount)
        for row in rows:
            #print(row)
            average = (row[0])
            minimum = (row[1])
            maximum = (row[2])
            #print('Ave ' + str(average))
            #print('Min ' + str(minimum))
            #print('Max ' + str(maximum))
            with open('publish.csv', 'ab') as csvfile:
                statwriter = csv.writer(csvfile, delimiter=',',
                                        quotechar='|', quoting=csv.QUOTE_MINIMAL)
                statwriter.writerow([datetime.datetime.now(),average, minimum, maximum])

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def get_newsletters():
    """ Query 7 day stats for the publish table"""
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("SELECT \
        ROUND(avg(EXTRACT(EPOCH FROM (n.mailout_finished::timestamp - n.mailout_started::timestamp))/60)::numeric,2) as average_Minutes, \
        ROUND(min(EXTRACT(EPOCH FROM (n.mailout_finished::timestamp - n.mailout_started::timestamp))/60)::numeric,2) as min_Minutes, \
        ROUND(max(EXTRACT(EPOCH FROM (n.mailout_finished::timestamp - n.mailout_started::timestamp))/60)::numeric,2) as max_Minutes \
    FROM \
        newsletters n, sites s \
    WHERE \
       scheduled > (CURRENT_TIMESTAMP - INTERVAL '7 day') and s.id = n.site_id \
    LIMIT 1000;")
        rows = cur.fetchall()
        #print("The number of rows is: ", cur.rowcount)
        for row in rows:
            #print(row)
            average = (row[0])
            minimum = (row[1])
            maximum = (row[2])
            #print('Ave ' + str(average))
            #print('Min ' + str(minimum))
            #print('Max ' + str(maximum))
            with open('newsletter.csv', 'ab') as csvfile:
                statwriter = csv.writer(csvfile, delimiter=',',
                                        quotechar='|', quoting=csv.QUOTE_MINIMAL)
                statwriter.writerow([datetime.datetime.now(),average, minimum, maximum])

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def get_rewrites():
    """ Query 7 day stats for the publish table"""
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("SELECT \
        ROUND(avg(EXTRACT(EPOCH FROM (finish_time::timestamp - start_time::timestamp))/60)::numeric,2) as average_Minutes,\
        ROUND(min(EXTRACT(EPOCH FROM (finish_time::timestamp - start_time::timestamp))/60)::numeric,2) as min_Minutes,\
        ROUND(max(EXTRACT(EPOCH FROM (finish_time::timestamp - start_time::timestamp))/60)::numeric,2) as max_Minutes\
    FROM\
        job_queue\
    WHERE\
        name ilike 'Regenerate Alias RewriteMap%' and scheduled_start > (CURRENT_TIMESTAMP - INTERVAL '7 day')\
    LIMIT 1000;")
        rows = cur.fetchall()
        #print("The number of rows is: ", cur.rowcount)
        for row in rows:
            #print(row)
            average = (row[0])
            minimum = (row[1])
            maximum = (row[2])
            #print('Ave ' + str(average))
            #print('Min ' + str(minimum))
            #print('Max ' + str(maximum))
            with open('rewrites.csv', 'ab') as csvfile:
                statwriter = csv.writer(csvfile, delimiter=',',
                                        quotechar='|', quoting=csv.QUOTE_MINIMAL)
                statwriter.writerow([datetime.datetime.now(),average, minimum, maximum])

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    #connect()
    get_publish()
    get_newsletters()
    get_rewrites()