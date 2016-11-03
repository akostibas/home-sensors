#!/usr/bin/python
import boto
import boto.s3.connection
import os
import shutil
import socket
import tempfile

from datetime import datetime

DATAFILE = '/mnt/sda1/data/robo-sensor.json'
DATAFILE_NAME, DATAFILE_EXT = os.path.splitext(DATAFILE)
DATAFILE_DIR = os.path.dirname(DATAFILE)
BUCKET = 'alexi-littlechina'
UPLOAD_DIR_PREFIX = 'upload_'
UPLOAD_DIR = '/data/' + socket.gethostname() + '/'
UPLOAD_FILENAME = socket.gethostname() + \
    '_' + \
    datetime.now().isoformat() + \
    DATAFILE_EXT

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

def upload_file_to_s3(filename):
    s3_conn = boto.connect_s3()
    bucket = s3_conn.get_bucket(BUCKET)
    
    print "Uploading to S3: " + filename
    upload = bucket.new_key(UPLOAD_DIR + UPLOAD_FILENAME)
    upload.set_contents_from_filename(
        filename,
        reduced_redundancy=True)

def main():
    # Ensure we can connect to S3 before anything else
    s3_conn = boto.connect_s3()
    s3_conn.head_bucket(BUCKET)
    
    # copy file to new location so that it is not being written to during
    # uplaod.
    copy_data_to_temp_dir()

    for file in find_files_to_upload():
        try:
            upload_file_to_s3(file)
            print "Cleaning up: " + file
            shutil.rmtree(os.path.dirname(file))
        except:
            print "Unable to upload: " + file
    
if __name__ == "__main__":
    main()
