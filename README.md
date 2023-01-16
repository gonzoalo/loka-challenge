# Loka Challenge(door2door)

The following projects aims to process data daily from events from a Car Service Company and stores them in a Data Warehouse For this purpose we are going to use AWS as our cloud provider so in order to run the project we need an aws account. 

## Architecture

For the infrastructure cloud architecture we are going to use a Raw and Strucutre zone. The first to copy the data from the original source and the second to store the data after the transformation job so we can still have a copy o the original data for any furutre use case. We are going to use Athena as a SQL-queriable data warehouse.

![alt text](img/architecture.png)

These services will be launched and setted with terraform an Infrastructure-as-code service which let us built everything we need to run the project. For the installation please refer the following [link](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli), otherwise you can launch the following services by yourself:
- S3 bucket with:
    - Raw Folder
    - Structrure Folder
    - Glue jobs Folder (with glue jobs inside)
- IAM Role for the glue jobs
- Glue Connection
- Glue Database

For the daily job execution we are going to use airflow as orchestrator with the following sequence of tasks:

![alt text](img/dags.png)

## Data

The data of the events are located in the bucket `s3://de-tech-assessment-2022/data/`


## Run the project

First of all you have to set the env variables in `.env` file

1. Create resources with terraform.

```
cd infra/terraform

terrafrom plan 

terraform apply
```

2. Run airflow on your local or virtual machine.

`docker-compose up `

Open your project [here](https://0.0.0.0:8080/home)

3. Set your aws credentials in airflow connections with the label `aws_default`. 

4. Run your dags on the airflow web service.

5. Go to Athena in your AWS account to see your data.

6. Start Querying!


## Future work

