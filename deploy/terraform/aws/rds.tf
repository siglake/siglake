resource "aws_security_group" "rds" {
  name        = "${var.name}-rds"
  description = "Postgres access from the siglake EKS cluster"
  vpc_id      = local.vpc_id
}

# Allow Postgres in from the EKS cluster's primary security group.
# The terraform-aws-modules/eks module creates a cluster SG with the
# node groups attached to it.
resource "aws_security_group_rule" "rds_from_cluster" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = aws_security_group.rds.id
  source_security_group_id = module.eks.cluster_security_group_id
  description              = "Postgres from EKS cluster SG"
}

resource "aws_security_group_rule" "rds_from_nodes" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = aws_security_group.rds.id
  source_security_group_id = module.eks.node_security_group_id
  description              = "Postgres from EKS node SG"
}

resource "aws_security_group_rule" "rds_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.rds.id
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_db_subnet_group" "rds" {
  name       = "${var.name}-rds"
  subnet_ids = local.private_subnet_ids
}

resource "aws_db_parameter_group" "rds" {
  name        = "${var.name}-pg16"
  family      = "postgres16"
  description = "siglake RDS Postgres parameter group"

  # rds.force_ssl=1 makes Postgres require TLS. iceberg-catalog-sql /
  # sqlx will negotiate TLS automatically as long as the URI uses
  # postgres://… (not postgresql+psycopg2 or similar non-libpq form).
  parameter {
    name  = "rds.force_ssl"
    value = "1"
  }
}

resource "random_password" "rds" {
  length  = 32
  special = false
}

resource "aws_db_instance" "rds" {
  identifier     = "${var.name}-catalog"
  engine         = "postgres"
  engine_version = var.rds_engine_version
  instance_class = var.rds_instance_class

  allocated_storage     = var.rds_allocated_storage
  max_allocated_storage = var.rds_max_allocated_storage
  storage_type          = "gp3"
  storage_encrypted     = true

  db_name  = var.rds_db_name
  username = var.rds_username
  password = random_password.rds.result

  db_subnet_group_name   = aws_db_subnet_group.rds.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  parameter_group_name   = aws_db_parameter_group.rds.name

  backup_retention_period = var.rds_backup_retention_days
  backup_window           = "03:00-04:00"
  maintenance_window      = "sun:04:00-sun:05:00"

  multi_az                     = false
  publicly_accessible          = false
  auto_minor_version_upgrade   = true
  deletion_protection          = var.rds_deletion_protection
  skip_final_snapshot          = !var.rds_deletion_protection
  final_snapshot_identifier    = var.rds_deletion_protection ? "${var.name}-catalog-final" : null
  performance_insights_enabled = true

  apply_immediately = true
}

# ---------------------------------------------------------------------------
# Secrets Manager: emit the credentials as a single secret so the
# Helm chart's `postgres.existingSecret` can wire to it via External
# Secrets Operator or the AWS Secrets Store CSI driver. The
# orchestration scripts also use this to create a vanilla k8s Secret.
# ---------------------------------------------------------------------------
resource "aws_secretsmanager_secret" "rds" {
  name        = "${var.name}-postgres"
  description = "siglake RDS Postgres credentials"

  # No recovery window: Secrets Manager normally keeps deleted secrets in
  # a "scheduled for deletion" state for 30 days. That collides with the
  # same name on the next `terraform apply` after a `terraform destroy`,
  # which is the standard smoke + iterate flow. Force immediate purge so
  # the round-trip works. Real customer deployments may want to bump this
  # back up to 7+ days for audit / accidental-delete protection.
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "rds" {
  secret_id = aws_secretsmanager_secret.rds.id
  secret_string = jsonencode({
    host     = aws_db_instance.rds.address
    port     = tostring(aws_db_instance.rds.port)
    user     = aws_db_instance.rds.username
    password = random_password.rds.result
    database = aws_db_instance.rds.db_name
  })
}
