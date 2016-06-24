#!/bin/sh
# Author: Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
# A wrapper script to submit spark job with pbr.py script

# test arguments
if [ "$#" -eq 0 ]; then
    echo "Usage: pbr <options>"
    echo "       pbr --help"
    exit 1
fi

# set avro jars
wdir=$PWD
avrojar=/usr/lib/avro/avro-mapred.jar
sparkexjar=/usr/lib/spark/examples/lib/spark-examples-1.6.0-cdh5.7.0-hadoop2.6.0-cdh5.7.0.jar

if [ "$1" == "-h" ] || [ "$1" == "--help" ] || [ "$1" == "-help" ]; then
    # run help
    python $wdir/pbr.py --help
elif [[  $1 =~ -?-yarn(-cluster)?$ ]]; then
    # to tune up these numbers:
    #  - executor-memory not more than 5G
    #  - num-executor can be increased (suggested not more than 10)
    #  - cores = 2/4/8
    # Temp solution to have a wrapper for python27 on spark cluster
    # once CERN IT will resolve python version we can remove PYSPARK_PYTHON
    PYSPARK_PYTHON='/afs/cern.ch/user/v/valya/public/python27' \
        spark-submit --jars $avrojar,$sparkexjar \
            --master yarn-client \
            --driver-class-path '/usr/lib/hive/lib/*' \
            --driver-java-options '-Dspark.executor.extraClassPath=/usr/lib/hive/lib/*' \
            $wdir/pbr.py ${1+"$@"}
else
    PYSPARK_PYTHON='/afs/cern.ch/user/v/valya/public/python27'
    spark-submit --jars $avrojar,$sparkexjar \
        --executor-memory $((`nproc`/4))G \
        --master local[$((`nproc`/4))] \
        --driver-class-path '/usr/lib/hive/lib/*' \
        --driver-java-options '-Dspark.executor.extraClassPath=/usr/lib/hive/lib/*' \
        $wdir/pbr.py ${1+"$@"}
fi
