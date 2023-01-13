# We are going to create a bucket with raw zones and structure zones for the data 

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.8.0"
    }
  }

  required_version = ">= 0.14.9"
}

provider "aws" {
    profile = "default"

}

# S3 resources
# for the 2 stages of the datalake raw and structured data 
# One folder for the glue jobs

resource "aws_s3_bucket" "main_bucket" {
  bucket = "de-tech-assessment-2022-gonzalo"

  tags = {
    Name        = "de-tech-assessment-2022-gonzalo"
    Environment = "Dev"
  }
}

resource "aws_s3_bucket_acl" "bucket_acl" {
  bucket = aws_s3_bucket.main_bucket.id
  acl    = "private"
}

resource "aws_s3_object" "raw_zone_folder" {
  bucket       = aws_s3_bucket.main_bucket.bucket
  acl          = "private"
  key          = "raw_zone/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "structure_zone_folder" {
  bucket       = aws_s3_bucket.main_bucket.bucket
  acl          = "private"
  key          = "structure_zone/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "glue_jobs_folder" {
  bucket       = aws_s3_bucket.main_bucket.bucket
  acl          = "private"
  key          = "glue_jobs/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "process_data_job" {
  bucket       = aws_s3_bucket.main_bucket.bucket
  acl          = "private"
  key          = "glue_jobs/process_data.py"
  source       = "../../src/glue_jobs/process_data.py"
}

# Glue resources
# DB connection and db

resource "aws_glue_catalog_database" "aws_glue_catalog_database" {
  name = "door2doordb2"
}

resource "aws_glue_connection" "aws_glue_conn" {
  name = "door2door_conn2"
  connection_type = "NETWORK"
  physical_connection_requirements {
    security_group_id_list = [var.security_group_id]
    subnet_id              = var.subnet_id
  }
}

# IAM resources
# role for glue actions

resource "aws_iam_role" "glue_role" {
  name = "AWSGlueServiceRoleDefault"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "glue_service" {
    role = "${aws_iam_role.glue_role.id}"
    policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "s3_policy" {
  name = "s3_policy"
  role = "${aws_iam_role.glue_role.id}"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:*"
      ],
      "Resource": [
        "arn:aws:s3:::my_bucket",
        "arn:aws:s3:::my_bucket/*"
      ]
    }
  ]
}
EOF
}

# resource "aws_iam_role_policy" "glue_service_s3" {
#  name = "glue_service_s3"
#     role = "${aws_iam_role.glue_role.id}"
#     policy = "${aws_iam_role_policy.s3_policy.policy}"
# }
