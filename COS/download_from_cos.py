import argparse
import ibm_boto3
import json
from ibm_botocore.client import Config


def download_file_cos(credentials, bucket, local_file_name, key):
    cos = ibm_boto3.client(
        service_name='s3',
        ibm_api_key_id=credentials['IBM_API_KEY_ID'],
        ibm_service_instance_id=credentials['IAM_SERVICE_ID'],
        config=Config(signature_version='oauth'),
        endpoint_url=credentials['ENDPOINT'])
    cos.download_file(
        Bucket=bucket,
        Key=key,
        Filename=local_file_name)
    print('File Downloaded')


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Upload file to COS.")
    parser.add_argument("--credentials", type=str, default="credentials.json",
                        help="JSON file with the required COS credentials.")
    parser.add_argument("--bucket", type=str, required=True,
                        help="Bucket name.")
    parser.add_argument("--fname", type=str, required=True,
                        help="File name on this machine to upload to COS.")
    parser.add_argument("--key", type=str, required=False,
                        help="key name. This is the identifier within COS. Defaults to fname.")
    args, _ = parser.parse_known_args()

    with open(args.credentials, "r") as fn:
        credentials = json.load(fn)

    key = args.key if args.key else args.fname

    download_file_cos(
        credentials=credentials,
        bucket=args.bucket,
        local_file_name=args.fname,
        key=key,
    )
