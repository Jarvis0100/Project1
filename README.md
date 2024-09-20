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

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node = glueContext.create_dynamic_frame.from_catalog(
    database="ecommerce", 
    table_name="final_bucket_project", 
    transformation_ctx="AWSGlueDataCatalog_node"
)

# Script generated for node SQL Query
SqlQuery = '''
select * from myDataSource where mpg=13
'''
SQLQuery_node = sparkSqlQuery(
    glueContext, 
    query=SqlQuery, 
    mapping={"myDataSource": AWSGlueDataCatalog_node}, 
    transformation_ctx="SQLQuery_node"
)

# Script generated for node Amazon Redshift
AmazonRedshift_node = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node, 
    connection_type="redshift", 
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-676957760376-ap-south-1/temporary/", 
        "useConnectionProperties": "true", 
        "dbtable": "public.car_data", 
        "connectionName": "Redshift connection", 
        "preactions": """
            CREATE TABLE IF NOT EXISTS public.car_data (
                mpg DOUBLE PRECISION, 
                cylinders BIGINT, 
                displacement DOUBLE PRECISION, 
                horsepower VARCHAR(256), 
                weight BIGINT, 
                acceleration DOUBLE PRECISION, 
                model_year BIGINT, 
                origin BIGINT, 
                carname VARCHAR(256)
            );
        """
    }, 
    transformation_ctx="AmazonRedshift_node"
)

job.commit()
============================
CREATE TABLE IF NOT EXISTS public.car_data (
    mpg DOUBLE PRECISION,
    cylinders BIGINT,
    displacement DOUBLE PRECISION,
    horsepower VARCHAR(256),
    weight BIGINT,
    acceleration DOUBLE PRECISION,
    model_year BIGINT,
    origin BIGINT,
    carname VARCHAR(256)
);
