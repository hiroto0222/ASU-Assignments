#!/usr/bin/python3
#
# Assignment2 Interface
#

import psycopg2
import os
import sys

# Do not close the connection inside this file i.e. do not perform openconnection.close()

def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    print("RangeQuery: {} <= ratingValue <= {}".format(ratingMinValue, ratingMaxValue))

    out = []
    conn = openconnection.cursor()

    # execute query against range partitions
    conn.execute(
        "SELECT PartitionNum FROM RangeRatingsMetaData WHERE MaxRating >= {} AND MinRating <= {}".format(ratingMinValue, ratingMaxValue)
    )
    rangePartitions = conn.fetchall()
    rangePartitions = [x[0] for x in rangePartitions]
    rangePartitionQuery = "SELECT * FROM RangeRatingsPart{} WHERE Rating >= {} AND Rating <= {}"

    for rangePartition in rangePartitions:
        conn.execute(rangePartitionQuery.format(rangePartition, ratingMinValue, ratingMaxValue))
        results = conn.fetchall()
        for res in results:
            res = ["RangeRatingsPart" + str(rangePartition)] + list(res)
            out.append(res)
    
    # execute query against round robin paritions
    conn.execute(
        "SELECT PartitionNum FROM RoundRobinRatingsMetadata"
    )
    rrobinPartiions = [i for i in range(conn.fetchall()[0][0])]
    rrobinPartitionQuery = "SELECT * FROM RoundRobinRatingsPart{} WHERE Rating >= {} AND Rating <= {}"
    
    for rrobinPartition in rrobinPartiions:
        conn.execute(rrobinPartitionQuery.format(rrobinPartition, ratingMinValue, ratingMaxValue))
        results = conn.fetchall()
        for res in results:
            res = ["RoundRobinRatingsPart" + str(rrobinPartition)] + list(res)
            out.append(res)
    
    writeToFile("RangeQueryOut.txt", out)


def PointQuery(ratingsTableName, ratingValue, openconnection):
    print("PointQuery: ratingValue = {}".format(ratingValue))

    out = []
    conn = openconnection.cursor()

    # execute query against range partitions
    conn.execute(
        "SELECT PartitionNum FROM RangeRatingsMetaData WHERE MaxRating >= {0} AND MinRating <= {0}".format(ratingValue)
    )
    rangePartitions = conn.fetchall()
    rangePartitions = [x[0] for x in rangePartitions]
    rangePartitionQuery = "SELECT * FROM RangeRatingsPart{} WHERE Rating = {}"

    for rangePartition in rangePartitions:
        conn.execute(rangePartitionQuery.format(rangePartition, ratingValue))
        results = conn.fetchall()
        for res in results:
            res = ["RangeRatingsPart" + str(rangePartition)] + list(res)
            out.append(res)
    
    # execute query against round robin partitions
    conn.execute(
        "SELECT PartitionNum FROM RoundRobinRatingsMetadata"
    )
    rrobinPartitions = [i for i in range(conn.fetchall()[0][0])]
    rrobinParitionQuery = "SELECT * FROM RoundRobinRatingsPart{} WHERE Rating = {}"

    for rrobinPartition in rrobinPartitions:
        conn.execute(rrobinParitionQuery.format(rrobinPartition, ratingValue))
        results = conn.fetchall()
        for res in results:
            res = ["RoundRobinRatingsPart" + str(rrobinPartition)] + list(res)
            out.append(res)
    
    writeToFile("PointQueryOut.txt", out)


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()