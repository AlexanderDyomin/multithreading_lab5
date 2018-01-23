from pyspark import SparkContext, Row
from pyspark.sql import SQLContext
import re

def hdfs(route):
    return "%s/%s" % (url, route)

def _extractor(line):
    #global log_pattern
    match = log_pattern.match(line)
    if match is None:
        raise Exception(line)
    return Row(**match.groupdict())

def logs():
    logs = sc.textFile(hdfs('/nasa/Jul')).map(_extractor)
    logs.cache()
    return logs

def create_frame(rdd):
    sql = SQLContext(sc)
    return sql.createDataFrame(rdd)

def save_frames(**kwargs):
    for k in kwargs:
        kwargs[k].write.mode('overwrite').json(hdfs(k))

# Sufficient pattern:
PATTERN     = '^(?P<host>\S+) - - \[(?P<datetime>.+)\] "((?P<method>\w+)\s+)?(?P<request>.+)" (?P<code>\d+) (?P<bytes>[\d\-]+)$'
log_pattern = re.compile(PATTERN)
(hdfs_host, hdfs_port) = ("my-hadoop-master", 9000)
url = 'hdfs://%s:%d' % (hdfs_host, hdfs_port)
sc  = SparkContext()

# Errors
error = logs().filter(lambda v: int(v.code) in range(500,600))

save_frames(errors=create_frame(error))
