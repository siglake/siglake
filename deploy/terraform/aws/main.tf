data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  # Take three AZs to give RDS multi-AZ headroom even though we don't
  # actually enable multi-AZ on the t4g.micro default. Customers who
  # bump rds_instance_class can flip the multi-AZ knob later.
  azs = slice(data.aws_availability_zones.available.names, 0, 3)

  warehouse_bucket = (
    var.warehouse_bucket_name != ""
    ? var.warehouse_bucket_name
    : "${var.name}-warehouse-${random_id.bucket_suffix.hex}"
  )

  cluster_name = "${var.name}-eks"
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# ---------------------------------------------------------------------------
# VPC
# ---------------------------------------------------------------------------
module "vpc" {
  count = var.create_vpc ? 1 : 0

  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.13"

  name = "${var.name}-vpc"
  cidr = var.vpc_cidr
  azs  = local.azs

  private_subnets = [for i, _ in local.azs : cidrsubnet(var.vpc_cidr, 4, i)]
  public_subnets  = [for i, _ in local.azs : cidrsubnet(var.vpc_cidr, 4, i + 8)]

  enable_nat_gateway   = true
  single_nat_gateway   = true
  enable_dns_hostnames = true

  # Tags required by the AWS load-balancer controller + EKS:
  public_subnet_tags = {
    "kubernetes.io/role/elb" = 1
  }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = 1
  }
}

locals {
  vpc_id             = var.create_vpc ? module.vpc[0].vpc_id : var.vpc_id
  private_subnet_ids = var.create_vpc ? module.vpc[0].private_subnets : var.private_subnet_ids
  public_subnet_ids  = var.create_vpc ? module.vpc[0].public_subnets : var.public_subnet_ids
}

# ---------------------------------------------------------------------------
# EKS
# ---------------------------------------------------------------------------
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.24"

  cluster_name    = local.cluster_name
  cluster_version = var.kubernetes_version

  cluster_endpoint_public_access = true

  enable_cluster_creator_admin_permissions = true

  vpc_id     = local.vpc_id
  subnet_ids = local.private_subnet_ids

  # EBS CSI is required for PVCs to provision. EFS CSI is optional and
  # left to the customer because EFS file-system provisioning is its
  # own can of worms (one-per-cluster vs. one-per-tenant, etc.).
  #
  # The EBS CSI controller needs IRSA — without `service_account_role_arn`
  # the controller pods fall back to the node IAM role, which doesn't
  # carry `ec2:DescribeAvailabilityZones` (or the other EBS perms). In
  # the 2026-05-16 smoke this left every PVC Pending and blocked the
  # workload install.
  cluster_addons = {
    coredns    = {}
    kube-proxy = {}
    vpc-cni    = {}
    aws-ebs-csi-driver = {
      service_account_role_arn = aws_iam_role.ebs_csi.arn
    }
    aws-efs-csi-driver = {
      service_account_role_arn = aws_iam_role.efs_csi.arn
    }
  }

  eks_managed_node_group_defaults = {
    ami_type = "AL2023_x86_64_STANDARD"
  }

  eks_managed_node_groups = {
    default = {
      instance_types = var.node_instance_types
      min_size       = var.node_min_size
      max_size       = var.node_max_size
      desired_size   = var.node_desired_size
    }
  }
}
