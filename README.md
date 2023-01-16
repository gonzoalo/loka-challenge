# Loka Challenge

the solution aims to process daily the data from the events in the car services 

We are going to build a module that will be executed every day and feeds the data warehouse

## Architecture

We will be using the following architecture:

![alt text](img/architecturefi.png)

![alt text](img/dags.png)

## Data

The data of the even s are located in the bucket s3://de-tech-assessment-2022/data/


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

Open on `localhost:8080/home`

3. Set your aws credentials in airflow connections with the label `aws_default`. 

4. Run your dags on the airflow web service.

5. Enter to Athena in AWS to see your data.


## Future work

