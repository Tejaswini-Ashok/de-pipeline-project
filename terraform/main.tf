provider "aws" {
  region = "eu-north-1"
}

# S3 bucket for raw logs
resource "aws_s3_bucket" "de_pipeline" {
  bucket = "de-pipeline-api-logs-tejj097"

  tags = {
    Project     = "de-pipeline"
    Environment = "dev"
  }
}

# Enable versioning
resource "aws_s3_bucket_versioning" "de_pipeline" {
  bucket = aws_s3_bucket.de_pipeline.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Folder structure
resource "aws_s3_object" "raw_folder" {
  bucket = aws_s3_bucket.de_pipeline.id
  key    = "raw/"
}

resource "aws_s3_object" "processed_folder" {
  bucket = aws_s3_bucket.de_pipeline.id
  key    = "processed/"
}

resource "aws_s3_object" "analytics_folder" {
  bucket = aws_s3_bucket.de_pipeline.id
  key    = "analytics/"
}

# Output
output "bucket_name" {
  value = aws_s3_bucket.de_pipeline.bucket
}

output "bucket_arn" {
  value = aws_s3_bucket.de_pipeline.arn
}