# -*- coding: utf-8 -*-
from __future__ import print_function
import re
from multiprocessing import Pool
from time import sleep
import logging
import os
from subprocess import call
import boto
#from boto.s3.key import Key

bucket_name = "lofar-elais-n1"

## Get URLs
file_surls = "retrieval/011/surls_successful.txt"
surls = [url.strip() for url in open(file_surls, "rb")]

#print(surls)


## Logging configuration
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)   
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
    
    # Check if the data is already in the bucket
    #logging.info("Check if {} is already in S3".format(name))
    #conn = boto.connect_s3()
    #bucket = conn.get_bucket(bucket_name)
    
    
    # Download the data
    command = "srmcp {srm} -globus_tcp_port_range=20000:25000 file://{path}/{f}".format(srm=url, path="/home/ubuntu", f=name)
    logging.info("Downloading {}".format(name))
    logging.debug(command)
    #call(command, shell=True)
    
    # Upload to the bucket
    logging.info("Uploading {} to S3".format(name))
    conn = boto.connect_s3()
    bucket = conn.get_bucket(bucket_name)
    key_name = "{}/{}".format(l, name)
    k = bucket.new_key(key_name)
    k.set_contents_from_filename("{path}/{f}".format(srm=url, path="/home/ubuntu", f=name))
    logging.info("Upload of {} to S3 finished".format(name))
    del conn
    


p = Pool(36)
p.map(download, surls)
