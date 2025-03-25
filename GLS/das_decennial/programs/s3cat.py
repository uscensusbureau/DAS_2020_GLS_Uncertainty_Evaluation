#!/usr/bin/env python3

"""s3_cat.py:

This program uses the AWS S3 API to concatenate all of the files
within a prefix into a single file.  The components are kept.

The original approach was to use the aws s3 API and combine objects on the back-end. Unfortunately, it turns out that there is a minimum part size of 5MB for S3 objects to be combined. In our testing, just using 'aws s3 cp --recursive' to download the parts, combine them locally, and then upload them together was superior. So that is one of the modes of operation, and it appears to be more resillient than the API-based approach.

References:
https://docs.aws.amazon.com/cli/latest/userguide/using-s3-commands.html
https://aws.amazon.com/blogs/developer/efficient-amazon-s3-object-concatenation-using-the-aws-sdk-for-ruby/
https://docs.aws.amazon.com/cli/latest/reference/s3api/create-multipart-upload.html
https://docs.aws.amazon.com/cli/latest/reference/s3api/upload-part-copy.html
https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPartCopy.html
https://docs.aws.amazon.com/cli/latest/reference/s3api/head-object.html
Note- minimum part size is 5MB

"""

import os


TMP_DIR = "/usr/tmp"

def get_tmp():
    if ("TMP" not in os.environ) and ("TEMP" not in os.environ) and ("TMP_DIR" not in os.environ):
        return TMP_DIR
    return None
