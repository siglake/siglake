data "aws_iam_policy_document" "warehouse_rw" {
  statement {
    sid = "BucketLevel"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [aws_s3_bucket.warehouse.arn]
  }

  statement {
    sid = "ObjectLevel"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.warehouse.arn}/*"]
  }
}

resource "aws_iam_policy" "warehouse_rw" {
  name        = "${var.name}-warehouse-rw"
  description = "Read/write access to the siglake Iceberg warehouse bucket"
  policy      = data.aws_iam_policy_document.warehouse_rw.json
}

# IRSA trust: a federated role that the cluster's OIDC provider can
# assume on behalf of the `siglake` ServiceAccount in the `siglake`
# namespace. The Helm chart's `serviceAccount.annotations` plugs the
# resulting role ARN onto that SA.
data "aws_iam_policy_document" "irsa_trust" {
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
      values   = ["system:serviceaccount:${var.name}:${var.name}"]
    }

    condition {
      test     = "StringEquals"
      variable = "${module.eks.oidc_provider}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "irsa" {
  name               = "${var.name}-warehouse-rw"
  assume_role_policy = data.aws_iam_policy_document.irsa_trust.json
}

resource "aws_iam_role_policy_attachment" "irsa_warehouse_rw" {
  role       = aws_iam_role.irsa.name
  policy_arn = aws_iam_policy.warehouse_rw.arn
}

# IRSA for the EBS CSI controller. Without this, the addon's controller
# pods fall back to the node IAM role, which doesn't carry the EBS
# permissions; PVCs end up Pending forever. The OIDC subject must
# match the controller SA exactly:
#   system:serviceaccount:kube-system:ebs-csi-controller-sa
data "aws_iam_policy_document" "ebs_csi_trust" {
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
      values   = ["system:serviceaccount:kube-system:ebs-csi-controller-sa"]
    }

    condition {
      test     = "StringEquals"
      variable = "${module.eks.oidc_provider}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ebs_csi" {
  name               = "${var.name}-ebs-csi-controller"
  assume_role_policy = data.aws_iam_policy_document.ebs_csi_trust.json
}

resource "aws_iam_role_policy_attachment" "ebs_csi" {
  role       = aws_iam_role.ebs_csi.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}
