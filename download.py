# -*- coding: utf-8 -*-
from __future__ import print_function
import re
from multiprocessing import Pool
from time import sleep
import logging
import os
from subprocess import call
import boto
from boto.exception import S3ResponseError

#from boto.s3.key import Key

bucket_name = "lofar-elais-n1"

## Get URLs
file_surls = "retrieval/011/surls_successful.txt"
surls = [url.strip() for url in open(file_surls, "rb")]

#print(surls)


## Logging configuration
logger = logging.getLogger()
logger.setLevel(logging.INFO)   
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
# Log to STDOUT
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)
# Log to file
file_name = "downloads.log"
fh = logging.FileHandler(file_name) 
fh.setFormatter(formatter)
logger.addHandler(fh)


## Define download sequence
#bucket = connection.get_bucket(options.bucket)



def download(url):
    path, name = os.path.split(url)
    l, sap, sb, suffix = name.split("_")
    key_name = "{}/{}".format(l, name)
    
    # Check if the data is already in the bucket
    logging.info("Check if {} is already in S3".format(name))
    conn = boto.connect_s3()
    bucket = conn.get_bucket(bucket_name)
    try:
        key = bucket.get_key(key_name)
        if key is None:
            local_file = "{path}/{f}".format(path="/home/ubuntu", f=name)
            
            if os.path.exists(local_file):
                # remove stale data
                command = "rm -rf {lf}".format(lf=local_file)
                logging.info("Remove old version of {}".format(name))
                logging.debug(command)
                call(command, shell=True)
            
            # Download the data
            command = "srmcp -server_mode=passive {srm} file:///{lf}".format(srm=url, lf=local_file)
            logging.info("Downloading {}".format(name))
            logging.debug(command)
            call(command, shell=True)
            
            if os.path.exists(local_file):
                # Upload to the bucket
                logging.info("Uploading {} to S3".format(name))
                k = bucket.new_key(key_name)
                k.set_contents_from_filename(local_file)
                logging.info("Upload of {} to S3 finished".format(name))
                
                # remove temporary data
                command = "rm -rf {lf}".format(lf=local_file)
                logging.info("Remove temporary version of {}".format(name))
                logging.debug(command)
                call(command, shell=True)
            else:
                logging.error("{} not found locally".format(name))
        else:
            logging.warn("{} is already in S3".format(name))
    except S3ResponseError:
        logging.error("Error querying {}".format(name))
    del conn

p = Pool(36)
p.map(download, surls)
