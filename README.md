# Loka Challenge

the solution aims to process daily the data from the events in the car services 

We are going to build a module that will be executed every day and feeds the data warehouse

## Strategy

- build infrastructure
- docker with airflow
- build dags


## Data

The data of the even s are located in the bucket s3://de-tech-assessment-2022/data/


## Run the project

First of all you have to set the env variables in `.env` file

1. Create resources with terraform.

` 
cd infra/terraform

terrafrom plan 

terraform apply
`

2. Run airflow on your local or virtual machine.

`docker-compose up `

Open on `localhost:8080/home`

3. Run your dags on the airflow web service.

4. Enter to Athena in AWS to see your data.
