import boto3


glue = boto3.client('glue')

glue.start_job_run(
    JobName='retviews_superstore_curated_to_aggregated',
    Arguments={}
)