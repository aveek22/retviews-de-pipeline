import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="retviews-demo", table_name="raw", transformation_ctx="S3bucket_node1"
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("row id", "long", "row_id", "long"),
        ("order id", "string", "order_id", "string"),
        ("ship mode", "string", "ship_mode", "string"),
        ("customer id", "string", "customer_id", "string"),
        ("customer name", "string", "customer_name", "string"),
        ("order date", "string", "order_date", "date"),
        ("ship date", "string", "ship_date", "date"),
        ("segment", "string", "segment", "string"),
        ("city", "string", "city", "string"),
        ("state", "string", "state", "string"),
        ("country", "string", "country", "string"),
        ("postal code", "double", "postal_code", "double"),
        ("market", "string", "market", "string"),
        ("region", "string", "region", "string"),
        ("product id", "string", "product_id", "string"),
        ("category", "string", "product_category", "string"),
        ("sub-category", "string", "product_sub_category", "string"),
        ("product name", "string", "product_name", "string"),
        ("sales", "double", "sales", "double"),
        ("quantity", "long", "quantity", "long"),
        ("discount", "double", "discount", "double"),
        ("profit", "double", "profit", "double"),
        ("shipping cost", "double", "shipping_cost", "double"),
        ("order priority", "string", "order_priority", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://retviews-demo/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="retviews-demo", catalogTableName="superstore_curated"
)
S3bucket_node3.setFormat("csv")
S3bucket_node3.writeFrame(ApplyMapping_node2)
job.commit()
