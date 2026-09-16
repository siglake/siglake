variable "region" {
  description = "AWS region for all siglake infrastructure."
  type        = string
  default     = "us-west-2"
}

variable "name" {
  description = "Name prefix applied to every resource (cluster, bucket, RDS, IAM)."
  type        = string
  default     = "siglake"
}

variable "tags" {
  description = "Additional tags merged into every resource."
  type        = map(string)
  default     = {}
}

# -----------------------------------------------------------------------------
# VPC
# -----------------------------------------------------------------------------
# The module ships with a `create_vpc` toggle so customers can either let it
# provision a small VPC for the cluster or wire it into an existing one.
variable "create_vpc" {
  description = "Provision a new VPC for the cluster. Set false to use an existing VPC (provide `vpc_id` + `subnet_ids`)."
  type        = bool
  default     = true
}

variable "vpc_cidr" {
  description = "CIDR for the chart-provisioned VPC. Ignored when create_vpc=false."
  type        = string
  default     = "10.42.0.0/16"
}

variable "vpc_id" {
  description = "Existing VPC ID. Required when create_vpc=false."
  type        = string
  default     = ""
}

variable "private_subnet_ids" {
  description = "Existing private subnet IDs for EKS nodes + RDS. Required when create_vpc=false."
  type        = list(string)
  default     = []
}

variable "public_subnet_ids" {
  description = "Existing public subnet IDs (used for NAT, LB). Optional when create_vpc=false."
  type        = list(string)
  default     = []
}

# -----------------------------------------------------------------------------
# EKS
# -----------------------------------------------------------------------------
variable "kubernetes_version" {
  description = "EKS control-plane Kubernetes version."
  type        = string
  default     = "1.35"
}

variable "node_instance_types" {
  description = "EC2 instance types for the default managed node group."
  type        = list(string)
  default     = ["m6i.large"]
}

variable "node_desired_size" {
  description = "Default node group desired size."
  type        = number
  default     = 3
}

variable "node_min_size" {
  description = "Default node group min size."
  type        = number
  default     = 2
}

variable "node_max_size" {
  description = "Default node group max size."
  type        = number
  default     = 6
}

# -----------------------------------------------------------------------------
# RDS Postgres (Iceberg catalog)
# -----------------------------------------------------------------------------
variable "rds_instance_class" {
  description = "RDS instance class. `db.t4g.micro` is the cheapest Graviton option; fine for dev/smoke."
  type        = string
  default     = "db.t4g.micro"
}

variable "rds_engine_version" {
  description = "RDS Postgres engine version. AWS retires minor versions on a rolling schedule — check `aws rds describe-db-engine-versions --engine postgres` before pinning. 16.3 was retired by mid-2026."
  type        = string
  default     = "16.10"
}

variable "rds_allocated_storage" {
  description = "Allocated storage (GB)."
  type        = number
  default     = 20
}

variable "rds_max_allocated_storage" {
  description = "Storage autoscaling ceiling (GB). 0 disables autoscaling."
  type        = number
  default     = 100
}

variable "rds_backup_retention_days" {
  description = "Days of automated backups RDS retains. PITR window."
  type        = number
  default     = 7
}

variable "rds_db_name" {
  description = "Postgres database name."
  type        = string
  default     = "siglake"
}

variable "rds_username" {
  description = "Postgres master user."
  type        = string
  default     = "siglake"
}

variable "rds_deletion_protection" {
  description = "Block `terraform destroy` from removing RDS. Turn off only for ephemeral smoke envs."
  type        = bool
  default     = false
}

# -----------------------------------------------------------------------------
# S3 warehouse
# -----------------------------------------------------------------------------
variable "warehouse_bucket_name" {
  description = "Bucket name for the Iceberg warehouse. Leave empty to auto-name as `<name>-warehouse-<random>`."
  type        = string
  default     = ""
}

variable "warehouse_lifecycle_days_to_glacier" {
  description = "Days after which old warehouse objects transition to Glacier Instant Retrieval. 0 disables lifecycle."
  type        = number
  default     = 0
}

# -----------------------------------------------------------------------------
# ECR pull-through cache (optional)
# -----------------------------------------------------------------------------
variable "create_ecr_pull_through" {
  description = "Create an ECR pull-through cache against GHCR so the cluster doesn't hit GHCR directly."
  type        = bool
  default     = false
}

variable "ghcr_credentials_arn" {
  description = "ARN of a Secrets Manager secret containing `{username, accessToken}` for ghcr.io. Required when create_ecr_pull_through=true."
  type        = string
  default     = ""
}
