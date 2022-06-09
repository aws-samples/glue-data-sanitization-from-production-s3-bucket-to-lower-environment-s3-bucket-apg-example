import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get the bucket name from options that passed in through CDK/Cloudformation
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "SOURCE_BUCKETNAME",
    "TARGET_BUCKETNAME"
])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://" + args["SOURCE_BUCKETNAME"] + "/"], "recurse": True},
    transformation_ctx="S3bucket_node1",
)

# Script generated for node DropProductionFields
DropProductionFields_node2 = DropFields.apply(
    frame=S3bucket_node1,
    paths=["invoiceId", "accountId"],
    transformation_ctx="DropProductionFields_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropProductionFields_node2,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://" + args["TARGET_BUCKETNAME"] + "/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
