import boto3


local_file = 'output/superstore_raw.csv'
bucket_name = 'retviews-demo'
s3_file = 'raw/superstore_raw.csv'


def upload_file_to_s3_raw():
    s3 = boto3.client('s3')

    try:
        print(f'Uploading file to S3: {bucket_name}')
        s3.upload_file(local_file, bucket_name, s3_file)
        print('Upload Successful')
    except Exception as error:
        print(f'Error: {error}')


def delete_file_from_s3_raw(bucket_name, s3_file):
    s3 = boto3.client('s3')

    try:
        s3.delete_object(Bucket=bucket_name, Key=s3_file)
        print(f'File deleted from S3.')
    except Exception as error:
        print(f'Error: {error}')


if __name__ == '__main__':
    upload_file_to_s3_raw()
    # delete_file_from_s3_raw(bucket_name, s3_file)