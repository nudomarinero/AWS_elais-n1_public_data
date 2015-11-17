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
import argparse
from filechunkio import FileChunkIO
import math


## Configuration
bucket_name = "lofar-elais-n1"


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


## Check files not uploaded
#def check_no(surls):
    #conn = boto.connect_s3()
    #bucket = conn.get_bucket(bucket_name)
    #uploaded = [key.name for key in bucket.list()]
    #del conn

    #surls_no = []
    #for url in surls:
        #path, name = os.path.split(url)
        #l, sap, sb, suffix = name.split("_")
        #key_name = "{}/{}".format(l, name)
        #if key_name not in uploaded:
            #surls_no.append(url)
    #return surls_no


## Define download sequence
def download(url, conf=None):
    path, name = os.path.split(url)
    l, sap, sb, suffix = name.split("_")
    key_name = "{}/{}".format(l, name)
    
    # Check if the data is already in the bucket
    logging.info("Check if {} is already in S3".format(name))
    if conf is None: #AWS
        conn = boto.connect_s3()
        local_file_path = "/home/ubuntu"
    else:
        conn = boto.connect_s3(
            aws_access_key_id = conf["access_key"],
            aws_secret_access_key = conf["secret_key"],
            host = conf["host"],
            port = conf.get("port", 80),
            is_secure = conf.get("is_secure", False),
            calling_format = boto.s3.connection.OrdinaryCallingFormat()
            )
        local_file_path = conf.get("local_file_path", "/home/ubuntu")
    
    bucket = conn.get_bucket(bucket_name)
    try:
        key = bucket.get_key(key_name)
        if key is None:
            local_file = "{path}/{f}".format(path=local_file_path, f=name)
            local_file_complete_flag = "{path}/{f}.complete".format(path=local_file_path, f=name)
            
            # Download data with srm or use present data
            if not (os.path.exists(local_file) and os.path.exists(local_file_complete_flag)):
                if os.path.exists(local_file):
                    # remove stale data
                    command = "rm -rf {lf}".format(lf=local_file)
                    logging.info("Remove old version of {}".format(name))
                    logging.debug(command)
                    call(command, shell=True)
                # Download the data
                command = "srmcp -server_mode=passive -retry_num=3 {srm} file:///{lf}".format(srm=url, lf=local_file)
                logging.info("Downloading {}".format(name))
                logging.debug(command)
                call(command, shell=True)
                if os.path.exists(local_file):
                    try:
                        size = os.path.getsize(local_file)
                        if size != 0:
                            command = "touch {lf}".format(lf=local_file_complete_flag)
                            call(command, shell=True)
                            logging.debug("Download of {} completed".format(name))
                        else:
                            # remove stale data
                            command = "rm -rf {lf}".format(lf=local_file)
                            logging.info("Remove failed version of {}".format(name))
                            logging.debug(command)
                            call(command, shell=True)
                    except OSError:
                        logging.error("{} with no local size!!".format(name))
                else:
                    logging.error("{} not found after download!".format(name))
            else:
                logging.warn("{} found locally".format(name))
            
            # Upload data to S3
            if os.path.exists(local_file):
                # Upload to the bucket
                #k = bucket.new_key(key_name)
                source_size = os.stat(local_file).st_size
                mp = bucket.initiate_multipart_upload(key_name)
                chunk_size = 52428800
                chunk_count = int(math.ceil(source_size / float(chunk_size)))
                logging.info("Uploading {} to S3".format(name))
                for j in range(chunk_count):
                    offset = chunk_size * j
                    nbytes = min(chunk_size, source_size - offset)
                    logging.debug("Uploading chunk {} of {} of {} to S3".format(j, chunk_count, name))
                    with FileChunkIO(local_file, 'r', offset=offset, bytes=nbytes) as fp:
                        mp.upload_part_from_file(fp, part_num=j+1)
                mp.complete_upload()
                #k.set_contents_from_filename(local_file)
                logging.info("Upload of {} to S3 finished".format(name))
                
                # remove temporary data
                command = "rm -rf {lf} {lfc}".format(lf=local_file, lfc=local_file_complete_flag)
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

#def multiprocess_download(surls, n_process=36, n_retry=10):
    #"""
    #Parallel download using multiprocessing.
    #Not working.
    #"""
    #for i in range(n_retry):
        #surls_no = check_no(surls)
        #n_no = len(surls_no)
        #if n_no > 0:
            #logging.info("Start download loop {}; downloading {} MSs".format(i, n_no))
            #p = Pool(n_process)
            #p.map(download, surls_no)
            #logging.info("End download loop {}".format(i))
        #else:
            #logging.info("Abort download loop {}; all the data already in S3".format(i))
            #break

if __name__ == "__main__":
    ### Get URLs
    #file_surls = "retrieval/011/surls_successful.txt"
    #surls = [url.strip() for url in open(file_surls, "rb")]
    ##print(surls)
    parser = argparse.ArgumentParser(description='Download srm file and upload it to S3')
    parser.add_argument('srm', help='SRM link')
    args = parser.parse_args()
    
    # Debug
    #print(args.srm)
    try:
        # Load the configuration parameters from a configuration file
        from configuration import conf
        download(args.srm, conf=conf)
    except ImportError:
        download(args.srm)
    
    # RUN: cat surls_no.txt | xargs -P 36 -n 1 python download.py
    

