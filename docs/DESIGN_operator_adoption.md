# Design: operator adoption of a live helm release

**Status: design only — no live attempts until the hermetic adoption
test passes.** Filed 2026-07-17 when the standing-cluster
modernization found the long-lived cluster helm-managed while the
GA'd operator had only ever been validated on clusters it CREATED.

## Problem

`SiglakeCluster` reconciliation renders its own six-tier resource set.
Pointing it at a namespace that already runs a helm-rendered siglake
would today either fight helm's objects (same names ⇒ ownership
conflicts, dueling updates) or double-deploy (different names ⇒ two
compactors claiming — safe via the claim table, but two ingesters
behind one Service is a traffic split with two WALs). Adoption must
be a HANDOVER, not a merge.

## Requirements

1. **Zero data loss**: WAL PVCs/EFS volumes and any sealed-uncommitted
   segments must survive the handover; the at-least-once + dedup
   machinery covers the transition window.
2. **Bounded downtime**: ingest may pause seconds (clients buffer/
   retry per OTLP semantics); query should stay up throughout.
3. **No orphan objects**: helm release state must end retired (not
   deleted-with-cascade into the live resources).
4. **Reversible until the commit point**: a pre-flight report and a
   dry-run mode; abort leaves helm management intact.

## Approach: annotate-then-inherit

Kubernetes-native adoption via ownership metadata, mirroring how helm
itself adopts (`meta.helm.sh/release-*` + `app.kubernetes.io/managed-by`):

1. **Pre-flight (`siglake-operator adopt --dry-run`)**: diff the
   operator's WOULD-RENDER set against the live helm objects — names,
   pod templates, PVC claims, Services, env. Report every divergence.
   Refuse when divergences touch identity fields (Service names, PVC
   names, WAL mount paths) unless the SiglakeCluster spec is first
   aligned to match.
2. **Spec synthesis**: generate a `SiglakeCluster` CR FROM the helm
   values (a `values→spec` converter, unit-tested against the chart's
   values.yaml surface). The operator renders the SAME resource names
   the chart used (a `nameTemplate: helm-compat` spec knob), so
   adoption is metadata-only.
3. **Handover commit**:
   a. `helm uninstall --no-hooks --keep-history` is NOT used —
      instead `kubectl annotate/label` flips
      `app.kubernetes.io/managed-by: siglake-operator` and removes
      `meta.helm.sh/release-*` from each object, then the helm
      RELEASE SECRET is deleted (helm forgets without cascading).
   b. Apply the CR; the operator reconciles — with names/templates
      aligned, the first pass is a no-op apply (server-side apply
      with the operator's field manager taking ownership).
   c. Status must reach Ready + SchemaMigrated without any pod
      restart when specs truly match; divergent fields roll pods
      normally.
4. **Abort path**: before (3a) nothing changed. (3a) has two halves and
   they abort differently. After the metadata flip but before the
   release secret is deleted, the objects are unmanaged (running fine)
   and helm's history is intact: re-adding `meta.helm.sh/release-*` and
   setting `app.kubernetes.io/managed-by: Helm` restores the old world.
   Once the secret is deleted, that alone leaves `helm list` empty —
   the release record has to come back too, which is why the runbook
   saves it (`kubectl get secret -l owner=helm,name=<release> -o yaml`)
   before deleting it. Without that backup the only way back is a
   `helm install` of the saved effective values over the re-annotated
   objects, and history restarts at revision 1.

## Hermetic validation (required before live use)

Extend the operator's integration suite with an ADOPTION test:
helm-template-render the chart into an envtest/kind cluster, run the
converter, execute the handover, assert (a) no pod deletions when
specs align, (b) a values divergence produces exactly one rolling
update, (c) helm release secret gone, (d) WAL PVC identity preserved,
(e) a second reconcile is a no-op.

## Open questions

- Chart/operator render drift: the operator renders StatefulSets for
  tiers the chart runs as Deployments (ingester/compactor). Adoption
  either (a) teaches the operator a `workloadKind` compat knob, or
  (b) accepts one controlled roll from Deployment→StatefulSet at
  handover (workload identity changes; PVCs must be re-bound — this
  is the riskiest item and argues for (a)).
- KEDA ScaledObjects target names — must follow the compat names.
- Multi-tenancy env (X-Scope-OrgID auth flags) — the converter must
  carry every chart env knob or refuse.
