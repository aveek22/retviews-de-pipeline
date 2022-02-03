import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="retviews-demo",
    table_name="superstore_curated",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node SQL
SqlQuery0 = """
SELECT
    *
FROM superstore_curated
"""
SQL_node1643573531362 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"superstore_curated": S3bucket_node1},
    transformation_ctx="SQL_node1643573531362",
)

# Script generated for node Amazon S3
AmazonS3_node1643571139374 = glueContext.getSink(
    path="s3://retviews-demo/aggregated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1643571139374",
)
AmazonS3_node1643571139374.setCatalogInfo(
    catalogDatabase="retviews-demo", catalogTableName="superstore_aggregated"
)
AmazonS3_node1643571139374.setFormat("glueparquet")
AmazonS3_node1643571139374.writeFrame(SQL_node1643573531362)
job.commit()
