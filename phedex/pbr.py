#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : pbr.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: example how to run aggregated query over phedex replicas data
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
        msg = "Input data file on HDFS, e.g. hdfs:///path/data/file"
        self.parser.add_argument("--fname", action="store",
            dest="fname", default="", help=msg)

def headers():
    "Headers for our HDFS file, i.e. schema"
    names = """now, dataset_name, dataset_id, dataset_is_open, dataset_time_create, dataset_time_update,block_name, block_id, block_files, block_bytes, block_is_open, block_time_create, block_time_update,node_name, node_id, br_is_active, br_src_files, br_src_bytes, br_dest_files, br_dest_bytes,br_node_files, br_node_bytes, br_xfer_files, br_xfer_bytes, br_is_custodial, br_user_group, replica_time_create, replica_time_update"""
    return [n.strip() for n in names.split(',')]

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()

    # setup spark/sql context to be used for communication with HDFS
    sc = SparkContext(appName="phedex_br")
    sc.setLogLevel("ERROR")
    sqlContext = SQLContext(sc)

    # read given file into RDD
    rdd = sc.textFile(opts.fname).map(lambda line: line.split(","))
    # create a dataframe out of RDD
    pdf = rdd.toDF(headers())
    pdf.show()
    print("pdf data type", type(pdf))
    pdf.printSchema()

    # example of aggregation
    ndf = pdf.withColumn("block_bytes_tmp", pdf.block_bytes.cast(IntegerType())).drop("block_bytes").withColumnRenamed("block_bytes_tmp", "block_bytes")
    res = ndf.filter("dataset_is_open='y'").groupBy().sum('block_bytes')
    print("open dataset size", res.collect())

if __name__ == '__main__':
    main()
