# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import boto
import argparse
import boto.s3.connection


## Configuration
#bucket_name = "lofar-elais-n1"

def get_keyname(name, tier1=False):
    """
    Get the keyname
    """
    if tier1:
        l, sb, suffix, hash_tar = name.split("_")
        hash_split = hash_tar.split(".")
        if hash_split[1] == "tar":
            suffix = suffix + ".tar"
        key_name = "{}_{}_{}".format(l, sb, suffix)
    else:
        l, sap, sb, suffix = name.split("_")
        key_name = "{}/{}".format(l, name)
    return key_name

## Check files not uploaded
def check_no(surls, conf=None, tier1=False, bucket_name="lofar-elais-n1"):
    if conf is None: #AWS
        conn = boto.connect_s3()
    else:
        conn = boto.connect_s3(
            aws_access_key_id = conf["access_key"],
            aws_secret_access_key = conf["secret_key"],
            host = conf["host"],
            port = conf.get("port", 80),
            is_secure = conf.get("is_secure", False),
            calling_format = boto.s3.connection.OrdinaryCallingFormat()
            )

    bucket = conn.get_bucket(bucket_name)
    uploaded = [key.name for key in bucket.list()]
    del conn

    surls_no = []
    for url in surls:
        path, name = os.path.split(url)
        key_name = get_keyname(name, tier1=tier1)
        if key_name not in uploaded:
            surls_no.append(url)
    return surls_no

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Check if a list of URLS is in S3')
    parser.add_argument('srm', help='SRM list')
    parser.add_argument('-t','--tier1', action="store_true", default=False, 
                        help='Use Tier1 name format')
    parser.add_argument('-b','--bucket', default="lofar-elais-n1", 
                        help='Bucket name')
    args = parser.parse_args()
    
    ## Get URLs
    #"retrieval/009/surls_successful.txt"
    file_surls = args.srm
    surls = [url.strip() for url in open(file_surls, "rb")]
    
    try:
        # Load the configuration parameters from a configuration file
        from configuration import conf
        surls_no = check_no(surls, conf=conf, tier1=args.tier1, bucket_name=args.bucket)
    except ImportError:
        surls_no = check_no(surls, tier1=args.tier1, bucket_name=args.bucket)
    
    if not args.tier1:
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
    else:
        for url in surls_no:
            print(url)
    