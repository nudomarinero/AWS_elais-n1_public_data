# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import boto
import argparse


## Configuration
bucket_name = "lofar-elais-n1"

## Check files not uploaded
def check_no(surls):
    conn = boto.connect_s3()
    bucket = conn.get_bucket(bucket_name)
    uploaded = [key.name for key in bucket.list()]
    del conn

    surls_no = []
    for url in surls:
        path, name = os.path.split(url)
        l, sap, sb, suffix = name.split("_")
        key_name = "{}/{}".format(l, name)
        if key_name not in uploaded:
            surls_no.append(url)
    return surls_no

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Check if a list of URLS is in S3')
    parser.add_argument('srm', help='SRM list')
    args = parser.parse_args()
    
    ## Get URLs
    #"retrieval/009/surls_successful.txt"
    file_surls = args.srm
    surls = [url.strip() for url in open(file_surls, "rb")]
    surls_no = check_no(surls)
    
    # Order
    surls_final = []
    sn_else = []
    for url in surls_no:
        path, name = os.path.split(url)
        l, sap, sb, suffix = name.split("_")
        local_file = "{path}/{f}".format(path="/home/ubuntu", f=name)
        if os.path.exists(local_file):
            surls_final.append(url)
        else:
            sn_else.append(url)
    surls_final.extend(sn_else)
        
    
    for url in surls_final:
        print(url)
    