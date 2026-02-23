locals {
  azs = ["${var.region}a", "${var.region}c", "${var.region}d"]
}

resource "aws_iam_policy" "node_s3_access" {
  name        = "${var.cluster_name}-node-s3-access"
  description = "Allow EKS nodes to access S3 for Flink checkpoints"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = "arn:aws:s3:::dev-sclee01-apne1"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:DeleteObject"
        ]
        Resource = "arn:aws:s3:::dev-sclee01-apne1/*"
      }
    ]
  })
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = "${var.cluster_name}-vpc"
  cidr = "10.0.0.0/16"

  azs             = local.azs
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]

  map_public_ip_on_launch = true    

  enable_nat_gateway = false
  single_nat_gateway = false

  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    project = var.cluster_name
  }
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = var.cluster_name
  cluster_version = "1.30"

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.public_subnets

  enable_irsa = true

  cluster_addons = {
    aws-ebs-csi-driver = {
      most_recent = true
    }
  }

  access_entries = {
    ec2_admin = {
      principal_arn = "arn:aws:iam::967157097094:role/dataeng-sclee01-iam-roles"

      policy_associations = {
        admin = {
          policy_arn = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
          access_scope = {
            type = "cluster"
          }
        }
      }
    }
  }


  eks_managed_node_groups = {
    main = {
      name           = "${var.cluster_name}-ng"
      instance_types = ["m6i.xlarge"]

      desired_size = 4
      min_size     = 3
      max_size     = 5

      disk_size = 100

      iam_role_additional_policies = {
        s3_access     = aws_iam_policy.node_s3_access.arn
        ebs_csi       = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
      }
    }
  }

  tags = {
    project = var.cluster_name
  }

  cluster_endpoint_public_access  = true
  cluster_endpoint_private_access = true 
  cluster_endpoint_public_access_cidrs = [
      "54.95.132.75/32"
  ]
}