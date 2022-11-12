#!/usr/bin/python3
#
# Assignment2 Interface
#

import psycopg2
import os
import sys

# Do not close the connection inside this file i.e. do not perform openconnection.close()

def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    out = []
    conn = openconnection.cursor()

    # execute query against range partitions
    conn.execute(
        "SELECT PartitionNum FROM RangeRatingsMetaData WHERE MaxRating >= {} AND MinRating <= {}".format(ratingMinValue, ratingMaxValue)
    )
    rangePartitions = conn.fetchall()
    rangePartitions = [x[0] for x in rangePartitions]
    rangeParitionQuery = "SELECT * FROM RangeRatingsPart{} WHERE Rating >= {} AND Rating <= {}"

    for rangeParition in rangePartitions:
        conn.execute(rangeParitionQuery.format(rangeParition, ratingMinValue, ratingMaxValue))
        results = conn.fetchall()
        for res in results:
            res = ["RangeRatingsPart" + str(rangeParition)] + list(res)
            out.append(res)
    
    # execute query against round robin paritions
    conn.execute(
        "SELECT PartitionNum FROM RoundRobinRatingsMetadata"
    )
    rrobinPartiions = [i for i in range(conn.fetchall()[0][0])]
    rrobinParitionQuery = "SELECT * FROM RoundRobinRatingsPart{} WHERE Rating >= {} AND Rating <= {}"
    
    for rrobinPartition in rrobinPartiions:
        conn.execute(rrobinParitionQuery.format(rrobinPartition, ratingMinValue, ratingMaxValue))
        results = conn.fetchall()
        for res in results:
            res = ["RoundRobinRatingsPart" + str(rrobinPartition)] + list(res)
            out.append(res)
    
    writeToFile("RangeQueryOut.txt", out)


def PointQuery(ratingsTableName, ratingValue, openconnection):
    pass


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()