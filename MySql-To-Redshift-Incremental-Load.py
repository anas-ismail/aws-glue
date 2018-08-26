import sys
import boto3
import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME'])

sc = (sc if 'sc' in vars() else SparkContext())
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Source data info
source_catalog_db = 'catalog_db' # DB name in Glue Catalog
source_catalog_tbl = 'source_tbl' # Table name in Glue Catalog

# Target data info
target_catalog_connection = 'redshift-catalog-connection'
target_actual_db = 'redshift_db' # Actual DB name in redshift
target_actual_tbl = 'target_tbl' # Actual Table name in redshift

# Region name
region = 'us-east-1'

# Table name for checkpointing in DynamoDB
tbl_name = 'target_tbl' 

# Last modified column for checkpointing in DynamoDB
lastModifiedColumn = 'time_stamp'

# Dynamo db table for checkpoints
dynamo_tbl = 'CHECKPOINT_TBL'


# Function: Get Last Checkpoint from DynamoDB
def getLastCheckPoint(client, tablename):
    response = client.get_item(TableName=dynamo_tbl,
                               Key={'TABLE_NAME': {'S': tablename}})
    checkpoint = response['Item']['LAST_CHECKPOINT']['S']
    return int(checkpoint)


# Function: Update Checkpoint in DynamoDB
def updateCheckPoint(
    client,
    tablename,
    lastcheckpoint,
    status,
    ):

    response = client.put_item(TableName=dynamo_tbl,
                               Item={'TABLE_NAME': {'S': tablename},
                               'LAST_CHECKPOINT': {'S': lastcheckpoint},
                               'STATUS': {'S': status}})
    return True


# Create dynamodb table
def createDynamodbTable():

    table = dynamodb.create_table(TableName=dynamo_tbl,
                                  KeySchema=[{'AttributeName': 'TABLE_NAME'
                                  , 'KeyType': 'HASH'}],
                                  AttributeDefinitions=[{'AttributeName': 'TABLE_NAME'
                                  , 'AttributeType': 'S'}],
                                  ProvisionedThroughput={'ReadCapacityUnits': 5,
                                  'WriteCapacityUnits': 5})

    # Wait for the table to exist before exiting
    print ('Waiting for', dynamo_tbl, '...')
    waiter = dynamodb.get_waiter('table_exists')
    waiter.wait(TableName=dynamo_tbl)


# Dynamodb client
dynamodb = boto3.client('dynamodb', region_name=region)

# Get an array of table names associated with the current account and endpoint.
response = dynamodb.list_tables()

if dynamo_tbl in response['TableNames']:
    table_found = True
else:
    createDynamodbTable()
    updateCheckPoint(dynamodb, tbl_name, str(0), 'No Executions yet')

# Last Checkpoint
lastcheckpoint = getLastCheckPoint(dynamodb, tbl_name)


# Mapping/Transformation function
def map_function(dynamicRecord):

    if dynamicRecord['old_column'] == 'something':
        myval = 'new_something'
    else:
        myval = dynamicRecord['old_column']

    dynamicRecord['new_column'] = myval
    return dynamicRecord

# Create dynamic dataframe from source data
datasource = glueContext.create_dynamic_frame.from_catalog(database=source_catalog_db,
                table_name=source_catalog_tbl, 
                redshift_tmp_dir=args['TempDir'], transformation_ctx='datasource')

#Filter data after last checkpoint
incremental_dyf = Filter.apply(frame=datasource, 
                                f=lambda x: x[lastModifiedColumn] > int(lastcheckpoint))


number_of_rows = incremental_dyf.count()

if number_of_rows > 0:
    mapped_dyf = Map.apply(frame=incremental_dyf, f=map_function,
                           transformation_ctx='mapped_dyf')

    # Find New Checkpoint
    df1 = mapped_dyf.toDF()
    newcheckpoint = df1.agg({lastModifiedColumn: 'max'}).collect()[0][0]

    # Write data to target table
    datasink = glueContext.write_dynamic_frame.from_jdbc_conf(frame=mapped_dyf,
                catalog_connection=target_catalog_connection,
                connection_options={'database': target_actual_db,
                'dbtable': target_actual_tbl},
                redshift_tmp_dir=args['TempDir'],
                transformation_ctx='datasink')

    # Update Checkpoint in DynamoDB
    updateCheckPoint(dynamodb, tbl_name, str(newcheckpoint), 'COMPLETED'
                     )

job.commit()
