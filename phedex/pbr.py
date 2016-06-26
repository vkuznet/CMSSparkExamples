#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : pbr.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description:
    http://stackoverflow.com/questions/29936156/get-csv-to-spark-dataframe
"""

# system modules
import os
import sys
import argparse

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import split as pysplit
from pyspark.sql.types import DoubleType, IntegerType

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
#        msg = "Input data location on HDFS, e.g. hdfs:///path/data"
#        self.parser.add_argument("--hdir", action="store",
#            dest="hdir", default="", help=msg)
        msg = "Input data file on HDFS, e.g. hdfs:///path/data/file"
        self.parser.add_argument("--fname", action="store",
            dest="fname", default="", help=msg)
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="Be verbose")
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="Be yarn")

def headers():
    names = """now, dataset_name, dataset_id, dataset_is_open, dataset_time_create, dataset_time_update,block_name, block_id, block_files, block_bytes, block_is_open, block_time_create, block_time_update,node_name, node_id, br_is_active, br_src_files, br_src_bytes, br_dest_files, br_dest_bytes,br_node_files, br_node_bytes, br_xfer_files, br_xfer_bytes, br_is_custodial, br_user_group, replica_time_create, replica_time_update"""
    return [n.strip() for n in names.split(',')]

def stats(iterable):
    "Helper function to perform aggregation action on iterable object"
    nfiles = 0
    bsize = 0
    dstatus = []
    cust = []
    group = []
    for row in iterable:
        nfiles += row.block_files
        bsize += row.block_bytes
        dstatus.append(row.dataset_is_open)
        cust.append(row.br_is_custodial)
        group.append(row.br_user_group)
    dataset_status = ','.join([str(i) for i in set(dstatus)])
    custodial = ','.join([str(i) for i in set(cust)])
    phedex_group = ','.join([str(i) for i in set(group)])
    return nfiles, bsize, dataset_status, custodial, phedex_group

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()

    # setup spark/sql context to be used for communication with HDFS
    sc = SparkContext(appName="phedex_br")
    if  not opts.yarn:
        sc.setLogLevel("ERROR")
    sqlContext = SQLContext(sc)

    # read given file into RDD
    rdd = sc.textFile(opts.fname).map(lambda line: line.split(","))
    # create a dataframe out of RDD
    pdf = rdd.toDF(headers())
    if  opts.verbose:
        pdf.show()
        print("pdf data type", type(pdf))
        pdf.printSchema()

    # cast columns to correct data types
    ndf = pdf.withColumn("block_bytes_tmp", pdf.block_bytes.cast(DoubleType()))\
             .drop("block_bytes").withColumnRenamed("block_bytes_tmp", "block_bytes")\
             .withColumn("block_files_tmp", pdf.block_files.cast(IntegerType()))\
             .drop("block_files").withColumnRenamed("block_files_tmp", "block_files")\
             .withColumn("br_src_bytes_tmp", pdf.br_src_bytes.cast(DoubleType()))\
             .drop("br_src_bytes").withColumnRenamed("br_src_bytes_tmp", "br_src_bytes")\
             .withColumn("br_src_files_tmp", pdf.br_src_files.cast(IntegerType()))\
             .drop("br_src_files").withColumnRenamed("br_src_files_tmp", "br_src_files")\
             .withColumn("br_dest_bytes_tmp", pdf.br_dest_bytes.cast(DoubleType()))\
             .drop("br_dest_bytes").withColumnRenamed("br_dest_bytes_tmp", "br_dest_bytes")\
             .withColumn("br_dest_files_tmp", pdf.br_dest_files.cast(IntegerType()))\
             .drop("br_dest_files").withColumnRenamed("br_dest_files_tmp", "br_dest_files")\
             .withColumn("br_node_bytes_tmp", pdf.br_node_bytes.cast(DoubleType()))\
             .drop("br_node_bytes").withColumnRenamed("br_node_bytes_tmp", "br_node_bytes")\
             .withColumn("br_node_files_tmp", pdf.br_node_files.cast(IntegerType()))\
             .drop("br_node_files").withColumnRenamed("br_node_files_tmp", "br_node_files")\
             .withColumn("br_xfer_bytes_tmp", pdf.br_xfer_bytes.cast(DoubleType()))\
             .drop("br_xfer_bytes").withColumnRenamed("br_xfer_bytes_tmp", "br_xfer_bytes")\
             .withColumn("br_xfer_files_tmp", pdf.br_xfer_files.cast(IntegerType()))\
             .drop("br_xfer_files").withColumnRenamed("br_xfer_files_tmp", "br_xfer_files")
    # example of aggregation
#    res = ndf.filter("dataset_is_open='y'").groupBy().sum('block_bytes')
#    print("open dataset size", res.collect())

    res = ndf.map(lambda r: ((r.dataset_name, r.node_name), r)).groupByKey().map(lambda g: (g[0], stats(g[1])))
    count = 0
    print("dataset site nfiles bsize status cust group")
    for item in res.collect():
        pair = item[0]
        dataset = pair[0]
        site = pair[1]
        items = item[1]
        nfiles = items[0]
        bsize = items[1]
        dstatus = items[2]
        cust = items[3]
        group = items[4]
        print('%s %s %s %s %s %s %s' % (dataset, site, nfiles, bsize, dstatus, cust, group))
        count += 1
        if count>10:
            break

if __name__ == '__main__':
    main()
