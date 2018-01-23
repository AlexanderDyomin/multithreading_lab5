from pyspark import SparkContext, Row
from pyspark.sql import SQLContext, Window
from pyspark.sql.functions import first, sum
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

def date_request(r):
     return Row(r.datetime[:11], 1)

def error_codes(v):
    return int(v.code) in range(400, 600)

# Sufficient pattern:
PATTERN     = '^(?P<host>\S+) - - \[(?P<datetime>.+)\] "((?P<method>\w+)\s+)?(?P<request>.+)" (?P<code>\d+) (?P<bytes>[\d\-]+)$'
log_pattern = re.compile(PATTERN)
(hdfs_host, hdfs_port) = ("my-hadoop-master", 9000)
url = 'hdfs://%s:%d' % (hdfs_host, hdfs_port)
sc  = SparkContext()

errors = logs().filter(error_codes).map(date_request).groupByKey().mapValues(len)
win    = Window.partitionBy("_1").orderBy("_1").rowsBetween(0, 6)
frame  = create_frame(errors).select(
        first('_1').over(win).alias("_1"),
        sum('_2').over(win).alias('_2')
)

save_frames(frames=frame)
