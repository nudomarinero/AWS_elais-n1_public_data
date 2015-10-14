# AWS_elais-n1_public_data
Tools to help with the upload of the LOFAR ELAIS-N1 public data to the S3 service of the Amazon Cloud.

There are 20 datasets at the moment corresponding to observations of both cycle 0 and 2. The list of files is stored in the directory ```lists```.

## Usage

After staging the data and creating the voms proxy certificate, the list of input URLs is entered to the command ```check_S3.py```. It filters the input list and outputs the files that are not yet in S3.

The list of URLs can be entered to ```download.py``` to download the data and store it in S3. This can be done in parallel using the command ```xargs```.

Example:
```
python check_S3.py retrieval/003/surls_successful.txt | tee surls_no_003.txt
cat surls_no_003.txt | xargs -P 36 -n 1 python download.py
```
