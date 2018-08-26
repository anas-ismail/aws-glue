# AWS Glue ETL Job
MySQL to Redshift Incremental Load by Timestamp (Checkpointing via DynamoDB)

## Short Description
Script takes loads data from a JDBC source into redshift while maintaining checkpoints in DynamoDB table to avoid duplication of data upon re-running the script for incoming data in the source.

## Reason for writing the script
Glue ETL Job does not support incremental load (Job bookmarks in Glue's language) for any source other than S3 and I was unable to find any implementation over the internet to achieve it.

## Architecture Diagram
![Architecture Diagram](https://github.com/anas-ismail/aws-glue/blob/master/images/architecture-diagram.png)

## Step By Step Guide
1. Setup the Source (MySql) connection in AWS Glue console
   - Follow the instructions to provide the required connection details
   - Test the connection
2. Create and run Glue Crawler on the source table using the connection created in previous step
3. Create a target Redshift cluster with appropriate security group and cluster subnet group attached
   - Security group must have port 5439 (default port) open for Inbound access
   - Assign or create a subnet group for the cluster
   - ETL job automatically creates target table and schema in redshift, but for optimizing table size, create a schema and table manually
4. Edit the MySql to Redshift ETL Job with required parameters i.e; source & target connection details and dynamodb table name
   - To run for any JDBC source and target, parameters at the top of the script must be changed accordingly
   - To apply any transformation in the source data, the transformation function 'map_function' needs to be updated accordingly
5. Create a Glue ETL job with MySql to Redshift ETL Job script from Glue console
   - In the Job creation wizard, select the source connection created in the first step
   - Follow the instructions to complete the job creation
   - Edit the script and paste MySql to Redshift ETL Job script edited in the previous step
6. Create a trigger with the required interval and link the job created in previous step with it
7. Run the job and validate the results
   - Results can be validated by comparing the row counts of source and target

## Troubleshooting Guide
- Setup VPC endpoint for S3 in order to run Glue crawler on MySql
- Setup DynamoDB endpoint and associate it with subnets
