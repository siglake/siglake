# EFS for the WAL volume — the chart needs RWX so that ingester and
# compactor can share `<wal>/` while running on different nodes. EBS is
# RWO and forces those pods onto a single node; EFS lifts that
# constraint.
#
# Wiring:
#   1. EFS file system (general-purpose throughput, encrypted at rest).
#   2. Mount target per private subnet so pods in any AZ can mount.
#   3. Security group allowing NFS (TCP 2049) from the EKS node SG.
#   4. IRSA role for `kube-system/efs-csi-controller-sa` with the
#      AWS-managed `AmazonEFSCSIDriverPolicy`.
#   5. EFS CSI driver as an EKS managed addon (added to main.tf
#      cluster_addons map).
#
# The StorageClass that points at this file system is created by
# `deploy/aws/up.sh` post-apply (kubectl), since this module doesn't
# install a kubernetes provider.

resource "aws_efs_file_system" "wal" {
  creation_token   = "${var.name}-wal"
  performance_mode = "generalPurpose"
  throughput_mode  = "bursting"
  encrypted        = true

  tags = {
    Name = "${var.name}-wal"
  }
}

resource "aws_security_group" "efs" {
  name        = "${var.name}-efs"
  description = "NFS (2049) from EKS nodes to the WAL EFS file system"
  vpc_id      = local.vpc_id
}

resource "aws_security_group_rule" "efs_ingress_from_nodes" {
  type                     = "ingress"
  from_port                = 2049
  to_port                  = 2049
  protocol                 = "tcp"
  security_group_id        = aws_security_group.efs.id
  source_security_group_id = module.eks.node_security_group_id
  description              = "NFS from EKS node SG"
}

# `count = length(local.azs)` instead of `for_each` so the first apply
# doesn't choke on subnet IDs being apply-time-only (the VPC module
# hasn't run yet). The AZ list is a data-source lookup, resolved at
# plan time, so the count is statically known.
resource "aws_efs_mount_target" "wal" {
  count = length(local.azs)

  file_system_id  = aws_efs_file_system.wal.id
  subnet_id       = local.private_subnet_ids[count.index]
  security_groups = [aws_security_group.efs.id]
}

# IRSA for the EFS CSI controller.
data "aws_iam_policy_document" "efs_csi_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${module.eks.oidc_provider}:sub"
      values   = ["system:serviceaccount:kube-system:efs-csi-controller-sa"]
    }

    condition {
      test     = "StringEquals"
      variable = "${module.eks.oidc_provider}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "efs_csi" {
  name               = "${var.name}-efs-csi-controller"
  assume_role_policy = data.aws_iam_policy_document.efs_csi_trust.json
}

resource "aws_iam_role_policy_attachment" "efs_csi" {
  role       = aws_iam_role.efs_csi.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEFSCSIDriverPolicy"
}
