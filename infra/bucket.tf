resource "aws_s3_bucket" "bucket" {
  bucket = "bucket-mehdi-salim-bania"

  tags = local.tags
}