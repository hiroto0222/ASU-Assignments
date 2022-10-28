#!/usr/bin/python3
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    conn = openconnection.cursor()  # create connection with postgresql
    conn.execute("CREATE TABLE IF NOT EXISTS {DB} (UserID INT, MovieID INT, Rating FLOAT)".format(
        DB=ratingstablename))  # create ratings table
    openconnection.commit()

    with open(ratingsfilepath, "r") as file:  # copy data into table
        for row in file:
            [userId, movieId, rating, timestamp] = row.split("::")
            conn.execute("INSERT INTO {} VALUES ({}, {}, {})".format(
                ratingstablename, userId, movieId, rating))
    openconnection.commit()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    partition_prefix = "range_part"
    conn = openconnection.cursor()  # create connection with postgresql
    # create metatable to store number of partitions
    conn.execute(
        "CREATE TABLE IF NOT EXISTS range_meta(part INT, from_rating FLOAT, to_rating FLOAT)")

    for i in range(numberofpartitions):
        # create N equal range partitions [0-1] (inclusive), (1-2] (disclusive), (2-3], (3-4], (4-5]
        from_rating = i * float(5 / numberofpartitions)
        to_rating = (i + 1) * float(5 / numberofpartitions)
        table_name = partition_prefix + str(i)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS {DB} (UserID INT, MovieID INT, Rating FLOAT)".format(DB=table_name))
        openconnection.commit()

        if i == 0:
            conn.execute(
                "INSERT INTO {DB} SELECT * FROM {RATINGS} WHERE {RATINGS}.rating >= {from_rating} AND {RATINGS}.rating <= {to_rating}".format(
                    DB=table_name, RATINGS=ratingstablename, from_rating=from_rating, to_rating=to_rating
                )
            )
        else:
            conn.execute(
                "INSERT INTO {DB} SELECT * FROM {RATINGS} WHERE {RATINGS}.rating > {from_rating} AND {RATINGS}.rating <= {to_rating}".format(
                    DB=table_name, RATINGS=ratingstablename, from_rating=from_rating, to_rating=to_rating
                )
            )
    
        openconnection.commit()

        # insert meta data
        conn.execute("INSERT INTO range_meta VALUES ({part}, {from_rating}, {to_rating})".format(part=i, from_rating=from_rating, to_rating=to_rating))
        openconnection.commit()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    pass


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    pass

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()