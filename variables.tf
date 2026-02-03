variable "cluster_name" {
  description = "The name of the EKS cluster"
  type        = string
  default     = "streaming-benchmark"
}

variable "region" {
  type    = string
  default = "ap-northeast-1"
}