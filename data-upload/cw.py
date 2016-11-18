#!/usr/bin/python
# Simple script to relay metrics to CloudWatch.
#
# Usage:
#    $0 metric value [metric 2] [value 2] [metric n] [value n]


import boto
import os
import sys

from datetime import datetime

cloudwatch = boto.connect_cloudwatch()

def put_metric(metric_name, metric_value, dimensions=None):
    cloudwatch.put_metric_data(
        'littlechina-test',
        metric_name,
        metric_value,
        datetime.utcnow(),
        'Count',
        dimensions)

def main(argv):
    
    # Remove the command name from arg list
    argv.pop(0)
    
    while len(argv) > 0:
        metric = argv.pop(0)
        value = float(argv.pop(0))
        # print "%s %s" % (argv.pop(0), argv.pop(0))
        put_metric(metric, value)
         
if __name__ == "__main__":
    main(sys.argv)
