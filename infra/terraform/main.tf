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
  #   profile = "default"
  region     = "us-east-1"
  access_key = "AKIAQ2VUICBNLKHJRU5F"
  secret_key = "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"

}

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
