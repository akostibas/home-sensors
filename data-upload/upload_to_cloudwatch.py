#!/usr/bin/python

import boto
import json
import os
import shutil
import socket
import sys
import tempfile

from datetime import datetime

DATAFILE = '/mnt/sda1/data/tmp.json'
DATAFILE_NAME, DATAFILE_EXT = os.path.splitext(DATAFILE)
DATAFILE_DIR = os.path.dirname(DATAFILE)
UPLOAD_DIR_PREFIX = 'upload_'
UPLOAD_FILENAME = socket.gethostname() + \
    '_' + \
    datetime.now().isoformat() + \
    DATAFILE_EXT

cloudwatch = boto.connect_cloudwatch()

def copy_data_to_temp_dir():
    if not os.path.exists(DATAFILE):
        return None
    
    print "Moving data to temporary location..."
    temp_dir = tempfile.mkdtemp(
        prefix=UPLOAD_DIR_PREFIX,
        dir=DATAFILE_DIR)
    temp_file = os.path.join(temp_dir, UPLOAD_FILENAME)
    os.rename(DATAFILE, temp_file)
    return temp_file

def find_files_to_upload():
    files_to_upload = []
    for dir in os.listdir(DATAFILE_DIR):
        if dir.startswith(UPLOAD_DIR_PREFIX):
            upload_dir = os.path.join(
                DATAFILE_DIR,
                dir)
            for file in os.listdir(upload_dir):
                files_to_upload.append(
                    os.path.join(upload_dir, file))
    return files_to_upload

def parse_file(file):
    """
    Reads a given file and returns an array of objects.
    We expect 'file' to be lines of JSON objects.
    """
    print "opening file"
    data = []
    try:
        data_file = open(file, 'r')
    except Exception as e:
        print "Error opening file: %s" % e
        return data
    
    for line in data_file:
        try:
            data_line = json.loads(line)
            data.append(data_line)
        except Exception as e:
            print "Malformed line: '%s'" % line
            print "  Exception: %s" % e
            continue
    print "Loaded %s lines of data." % len(data)
    return data

def send_data_to_cloudwatch(data):
    for data_line in data:
        for key in data_line.keys():
            if key == "timestamp":
                continue
            put_metric(key, data_line[key], metric_timestamp=data_line['timestamp'])

def parse_timestamp(time_string):
    """ Turns a string into a UTC datetime object.
    Strings must be in this format: 2016-11-18t15:00:13 """
    
    year = int(time_string[0:4])
    month = int(time_string[5:7])
    day = int(time_string[8:10])
    hour = int(time_string[11:13])
    min = int(time_string[14:16])
    sec = int(time_string[17:19])
    
    timestamp = datetime(year, month, day, hour, min, sec)
    utc_timestamp = datetime.utcfromtimestamp(float(timestamp.strftime('%s')))
    
    return utc_timestamp
    
def put_metric(metric_name, metric_value, metric_timestamp=None, dimensions=None):
    utc_timestamp = parse_timestamp(metric_timestamp)
    # print "%s > Metric: '%s' | Value: '%s'" % (utc_timestamp, metric_name, metric_value)
    cloudwatch.put_metric_data(
        'littlechina-test',
        metric_name,
        metric_value,
        utc_timestamp,
        'Count',
        dimensions)

def process_file(file):
    data = parse_file(file)
    send_data_to_cloudwatch(data)

def main():
    # copy file to new location so that it is not being written to during
    # uplaod.
    copy_data_to_temp_dir()
    
    for file in find_files_to_upload():
    	process_file(file)
        print "Cleaning up: " + file
        shutil.rmtree(os.path.dirname(file))

if __name__ == "__main__":
    main()
