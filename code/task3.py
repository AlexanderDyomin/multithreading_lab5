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

# Time series
def date_request(r):
    return Row((r.datetime[:11], "%s %s" % (r.method, r.code)), 1)

def request_counter(requests):
    result = {}
    for r in requests:
        if r not in result:
            result[r] = 0
        result[r] += 1
    return result

def filter_requests(requests):
    result = {}
    for k in requests:
        if requests[k] >= 10:
            result[k] = requests[k]
    return result

def finalGroup(n):
    return Row(n[0][0], (n[0][1], n[1]))

def greater10(n):
    return n[1] >= 10

# Sufficient pattern:
PATTERN     = '^(?P<host>\S+) - - \[(?P<datetime>.+)\] "((?P<method>\w+)\s+)?(?P<request>.+)" (?P<code>\d+) (?P<bytes>[\d\-]+)$'
log_pattern = re.compile(PATTERN)
(hdfs_host, hdfs_port) = ("my-hadoop-master", 9000)
url = 'hdfs://%s:%d' % (hdfs_host, hdfs_port)
sc  = SparkContext()

dated = logs().map(date_request)\
    .groupByKey().mapValues(len).filter(greater10)\
    .map(finalGroup).groupByKey().mapValues(list)

save_frames(dates=create_frame(dated))
