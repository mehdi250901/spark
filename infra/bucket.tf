resource "aws_s3_bucket" "bucket" {
  bucket = "mehdi-buquet"

  tags = local.tags
}