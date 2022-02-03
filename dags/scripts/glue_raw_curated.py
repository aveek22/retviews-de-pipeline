import boto3


glue = boto3.client('glue')

glue.start_job_run(
    JobName='retviews_superstore_raw_to_curated',
    Arguments={}
)