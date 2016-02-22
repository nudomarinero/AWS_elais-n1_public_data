# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import boto
import argparse
import boto.s3.connection

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

def make_public(surls, conf=None, tier1=False, bucket_name="lofar-elais-n1"):
    """
    Make public the surls
    """
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
    
    published = []
    if surls is not None:
        for url in surls:
            path, name = os.path.split(url)
            key_name = get_keyname(name, tier1=tier1)
            if key_name in uploaded:
                key = bucket.lookup(key_name)
                key.set_acl('public-read')
                published.append(key_name)
    else:
        for key_name in uploaded:
            key = bucket.lookup(key_name)
            key.set_acl('public-read')
            published.append(key_name)
    
    return published
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Check if a list of URLS is in S3')
    parser.add_argument('srm', help='SRM list', default=None)
    parser.add_argument('-t','--tier1', action="store_true", default=False, 
                        help='Use Tier1 name format')
    parser.add_argument('-b','--bucket', default="lofar-elais-n1", 
                        help='Bucket name')
    args = parser.parse_args()
    
    ## Get URLs
    #"retrieval/009/surls_successful.txt"
    if args.srm is None:
        surls = None
    else:
        file_surls = args.srm
        surls = [url.strip() for url in open(file_surls, "rb")]
    
    try:
        # Load the configuration parameters from a configuration file
        from configuration import conf
        published = make_public(surls, conf=conf, tier1=args.tier1, bucket_name=args.bucket)
    except ImportError:
        published = make_public(surls, tier1=args.tier1, bucket_name=args.bucket)
        
    for key in published:
        print("http://s3.amazonaws.com/{}/{}".format(args.bucket, key))