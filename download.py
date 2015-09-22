# -*- coding: utf-8 -*-
from __future__ import print_function
import re
from multiprocessing import Pool
from time import sleep
import logging
import os
from subprocess import call, check_output
import boto
from boto.exception import S3ResponseError

#from boto.s3.key import Key

bucket_name = "lofar-elais-n1"
passive = True

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
            # Download the data
            if passive:
                command = "srmcp -server_mode=passive {srm} file:///{path}/{f}".format(srm=url, path="/home/ubuntu", f=name)
            else:
                command = "srmcp {srm} -globus_tcp_port_range=20000:25000 file:////{path}/{f}".format(srm=url, path="/home/ubuntu", f=name)
                "-array_of_client_networks="
            logging.info("Downloading {}".format(name))
            logging.debug(command)
            output = check_output(command, shell=True)
            logging.debug(output)
            
            # Upload to the bucket
            logging.info("Uploading {} to S3".format(name))
            k = bucket.new_key(key_name)
            k.set_contents_from_filename("{path}/{f}".format(srm=url, path="/home/ubuntu", f=name))
            logging.info("Upload of {} to S3 finished".format(name))
            
            # remove temporary data
            command = "rm -rf {path}/{f}".format(path="/home/ubuntu", f=name)
            logging.info("Downloading {}".format(name))
            logging.debug(command)
            call(command, shell=True)
        else:
            logging.warn("{} is already in S3".format(name))
    except S3ResponseError:
        logging.error("Error querying {}".format(name))
    del conn

p = Pool(36)
p.map(download, surls)
