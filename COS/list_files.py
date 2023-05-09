import argparse
import ibm_boto3
import json
from ibm_botocore.client import Config


def list_files_cos(credentials, bucket):
    cos = ibm_boto3.client(
        service_name='s3',
        ibm_api_key_id=credentials['IBM_API_KEY_ID'],
        ibm_service_instance_id=credentials['IAM_SERVICE_ID'],
        config=Config(signature_version='oauth'),
        endpoint_url=credentials['ENDPOINT'])
    return cos.list_objects_v2(Bucket=bucket)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Upload file to COS.")
    parser.add_argument("--credentials", type=str, default="credentials.json",
                        help="JSON file with the required COS credentials.")
    parser.add_argument("--bucket", type=str, required=True,
                        help="Bucket name.")
    args, _ = parser.parse_known_args()

    with open(args.credentials, "r") as fn:
        credentials = json.load(fn)

    res = list_files_cos(
        credentials=credentials,
        bucket=args.bucket,
    )
    for obj in res["Contents"]:
        print(f"{obj['Key']} ({obj['LastModified'].isoformat()})")
