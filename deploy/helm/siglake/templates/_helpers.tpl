{{/*
Common helpers for the siglake chart.

Naming:
  siglake.fullname      - release-scoped name root used by every object.
  siglake.componentName - per-component name suffix (`<fullname>-<role>`).
  siglake.serviceAccountName - the ServiceAccount used by every pod.

Labels:
  siglake.labels         - standard labels for every object.
  siglake.selectorLabels - subset used as Deployment selectors (must be
                          stable across rollouts to satisfy the
                          immutable selector constraint).

Per-component env:
  siglake.commonEnv      - postgres + S3 + RUST_LOG; included on every pod.
*/}}

{{- define "siglake.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "siglake.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "siglake.componentName" -}}
{{- $top := index . 0 -}}
{{- $role := index . 1 -}}
{{- printf "%s-%s" (include "siglake.fullname" $top) $role | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "siglake.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
siglake.queryPeerSrv - the SRV record the query pods resolve to discover each
other (#967). The headless Service publishes one SRV answer per READY pod on
its named `http` port, so membership follows the actual replica count instead
of the count that was rendered: a KEDA replica becomes eligible for shard work
after its readiness probe, with no rollout. Replaces the static
`--query-peers` list, which was built from query.replicas at render time and
therefore capped fan-out at that number however far the tier scaled.
*/}}
{{- define "siglake.queryPeerSrv" -}}
{{- $hl := include "siglake.componentName" (list . "query-headless") -}}
{{- printf "_http._tcp.%s.%s.svc.cluster.local" $hl .Release.Namespace -}}
{{- end -}}

{{/*
siglake.compactorScaleGuard - FAIL THE INSTALL for a compactor tier that can
hold more than one pod without the catalog claim, and for a claim with no
mirror to claim from. Called with the top-level context from the compactor
Deployment and from the compactor HPA, so neither door is open on its own.

The chart has always documented the coupling (`catalogClaim`: "Multi-pod-safe:
set `replicas > 1` once this is on") and never enforced it. Both halves fail
quietly, which is why they are render-time refusals rather than notes:

  - Above one pod with the claim off, each replica runs the whole maintenance
    loop — leveled rewrites, snapshot expiry, retention and orphan GC — against
    one table on optimistic-concurrency commits, so the pods spend their
    budget losing commit races to each other. Nothing reports it as a
    misconfiguration; the layout simply stops converging.
  - With the claim on and the mirror off, the drain reads the `wal_segments`
    catalog table and NEVER local `sealed/`, and rows land there only from an
    ingester that mirrors. Zero segments are claimed, compaction stops, and
    `siglake_compactor_sealed_pending` reads zero, because the local sealed
    count is not what the claim path looks at. The binary's own refusal is a
    weaker version of this one: it refuses `--catalog-claim` with an EMPTY
    mirror prefix (siglake-cli), and since #5880 `wal.mirror.enabled: false`
    does render the compactor an empty `SIGLAKE_WAL_MIRROR_PREFIX` — but that
    is a pod crash-looping after install rather than an install that stops,
    and it says nothing about the ingester half of the pair.

`autoscaling.compactor.maxReplicas` counts as a replica count: an HPA ceiling
above one reaches the same state a few minutes after install rather than at
install. There is no KEDA ScaledObject for the compactor, so those two values
are the whole scaling surface.

The third refusal is the compactor HPA's own custom metric under the claim
(#3718). `autoscaling.compactor.customMetric` renders `type: Pods` with an
`averageValue` target, and the Pods algorithm divides the summed metric by the
current pod count before dividing the target into it. In claim mode every
compactor publishes the WHOLE shared queue — `siglake_compactor_sealed_pending`
comes from `peek_pending`, a `COUNT(*)` over sealed, unclaimed rows with no
worker filter — so the average equals the total and the ratio carries the
current count with it: a fixed backlog of 8 against a target of 4 asks for 4
pods from 2 and 8 from 4. The chart cannot divide the reading itself; a correct
target needs one aggregated series behind an `Object` or `External` metric,
which means an adapter rule this chart does not own. Refusing beats rendering
an HPA whose ceiling is the only thing that stops it. CPU-only claim-mode
scaling still renders, and so does the ingester's custom metric, which is a
genuine per-pod rate. In filesystem mode the sealed count IS one pod's own, but
that mode is held at a single pod by the guard above, so the custom metric
buys no backlog scaling there either — it renders at `maxReplicas: 1` and
nothing more. `siglake-operator` is where multi-pod backlog scaling lives: it
divides the target into the shared queue once, whatever the replica count.
*/}}
{{- define "siglake.compactorScaleGuard" -}}
{{- $max := int .Values.compactor.replicas -}}
{{- $from := "compactor.replicas" -}}
{{- if .Values.autoscaling.compactor.enabled -}}
{{- $hpaMax := int .Values.autoscaling.compactor.maxReplicas -}}
{{- if gt $hpaMax $max -}}
{{- $max = $hpaMax -}}
{{- $from = "autoscaling.compactor.maxReplicas" -}}
{{- end -}}
{{- end -}}
{{- if and (gt $max 1) (not .Values.compactor.catalogClaim.enabled) -}}
{{- fail (printf "%s=%d would run more than one compactor pod while compactor.catalogClaim.enabled is false: the replicas share no claim table, so each one runs the full drain and maintenance loop against the same tables and they contend on every Iceberg commit. Set compactor.catalogClaim.enabled=true (it needs wal.mirror.enabled, which is the default), or hold the compactor at one pod (compactor.replicas=1 with autoscaling.compactor.enabled=false, or autoscaling.compactor.maxReplicas=1)." $from $max) -}}
{{- end -}}
{{- $cm := .Values.autoscaling.compactor.customMetric -}}
{{- if and .Values.autoscaling.compactor.enabled $cm $cm.enabled .Values.compactor.catalogClaim.enabled -}}
{{- fail "autoscaling.compactor.customMetric.enabled is true with compactor.catalogClaim.enabled: the HPA would render siglake_compactor_sealed_pending as a `type: Pods` metric with an averageValue target, but in claim mode every compactor publishes the whole shared sealed queue, not its own share. The Pods algorithm averages that total over the running pods, so the request scales with the current count instead of the backlog: 8 sealed segments against a target of 5 asks for 2 pods from 1 and 4 from 2, up to autoscaling.compactor.maxReplicas. Scaling the claim on the backlog needs one aggregated series behind an Object or External metric and the adapter rule that exposes it, which this chart does not render. Set autoscaling.compactor.customMetric.enabled=false to keep the claim's CPU-only HPA, or use siglake-operator, which divides the target into the shared queue once at any replica count. (The custom metric does render with the claim OFF, where each pod's sealed count is its own — but a compactor without the claim is refused above any pod count of one, so it renders at autoscaling.compactor.maxReplicas=1 and scales nothing.)" -}}
{{- end -}}
{{- if and .Values.compactor.catalogClaim.enabled (not .Values.wal.mirror.enabled) -}}
{{- fail "compactor.catalogClaim.enabled is true with wal.mirror.enabled false: the claim drain reads the `wal_segments` catalog table, whose rows are written only by an ingester that mirrors its sealed segments, and it never reads the local `sealed/` directory. Compaction would stop with no error and siglake_compactor_sealed_pending reading zero. Set wal.mirror.enabled=true, or turn the claim off and hold the compactor at one pod." -}}
{{- end -}}
{{- end -}}

{{/*
siglake.embeddedCompactorGuard - FAIL THE INSTALL for `--with-compactor` in
`ingester.extraArgs`. Called with the top-level context from the ingester
Deployment, which is the only template that can put that flag on a pod.

`ingest-server --with-compactor` gives the pod an in-process compactor, and it
is built with no catalog claim (crates/siglake-cli/src/main.rs, the
`with_compactor` arm: no `with_catalog_claim`), so its maintenance lease admits
every process that asks. The chart renders no claim arguments on the ingester
either, which is why this is a refusal and not the remedy the compactor tier
gets: there is no value that makes two embedded compactors divide the work.
Concurrent copies run the same drain and the same maintenance loop — leveled
rewrites, snapshot expiry, retention, orphan GC — against the same tables, on
optimistic-concurrency commits, and lose them to each other. That is what
siglake.compactorScaleGuard refuses on the compactor tier, reached through a
different value.

The refusal is unconditional because `ingester.replicas: 1` is not exclusivity:
the Deployment's RollingUpdate is maxSurge 1 / maxUnavailable 0, so every
rollout runs the outgoing and the incoming pod together. Narrowing the
rollout instead would mean the chart quietly trading ingest availability for a
flag the deployment does not support anyway. The message still names the wider
overlap when there is one (an explicit replica count, an HPA or KEDA ceiling, a
dedicated compactor beside it), because that is the setting the operator
recognises. `compactor.catalogClaim.enabled` does not waive any of it.
*/}}
{{- define "siglake.embeddedCompactorGuard" -}}
{{- $embedded := false -}}
{{- range (default (list) .Values.ingester.extraArgs) -}}
{{- $arg := toString . -}}
{{- if or (eq $arg "--with-compactor") (hasPrefix "--with-compactor=" $arg) -}}
{{- $embedded = true -}}
{{- end -}}
{{- end -}}
{{- if $embedded -}}
{{- $max := int .Values.ingester.replicas -}}
{{- $from := "ingester.replicas" -}}
{{- if and .Values.keda.enabled .Values.keda.ingester.enabled -}}
{{- /* KEDA owns spec.replicas on this tier, so ingester.replicas is not even
     rendered; its ceiling replaces the count rather than competing with it. */ -}}
{{- $max = int .Values.keda.ingester.maxReplicas -}}
{{- $from = "keda.ingester.maxReplicas" -}}
{{- end -}}
{{- if .Values.autoscaling.ingester.enabled -}}
{{- $hpaMax := int .Values.autoscaling.ingester.maxReplicas -}}
{{- if gt $hpaMax $max -}}
{{- $max = $hpaMax -}}
{{- $from = "autoscaling.ingester.maxReplicas" -}}
{{- end -}}
{{- end -}}
{{- $why := list -}}
{{- if gt $max 1 -}}
{{- $why = append $why (printf "%s=%d holds more than one ingester pod" $from $max) -}}
{{- end -}}
{{- if .Values.compactor.enabled -}}
{{- $why = append $why "compactor.enabled=true runs a dedicated compactor over the same tables" -}}
{{- end -}}
{{- $why = append $why "the ingester rollout is maxSurge=1 with maxUnavailable=0, so the outgoing and incoming pods overlap even at ingester.replicas=1" -}}
{{- fail (printf "ingester.extraArgs contains --with-compactor, which gives every ingester pod an in-process compactor, and these compactors would run concurrently: %s. They share no claim: the embedded compactor is built without one, and the chart renders no claim arguments on the ingester, so each copy runs the whole drain and maintenance loop (leveled rewrites, snapshot expiry, retention, orphan GC) against the same tables and they contend on every Iceberg commit. compactor.catalogClaim.enabled does not cover this path. Drop --with-compactor from ingester.extraArgs and compact with the dedicated tier (compactor.enabled=true), which is where the claim and its scaling guard live." (join "; " $why)) -}}
{{- end -}}
{{- end -}}

{{/*
siglake.queryJobsScaleGuard - FAIL THE INSTALL when an enabled query tier can
hold more than one pod but each pod keeps its own in-memory batch-job store.

The ordinary Service routes every status, result and cancel read to any ready
pod. Without the shared store, a healthy job therefore reads as 404 whenever
the request reaches a pod other than its executor. Client affinity would not
fix pod replacement or clients behind different egress addresses, so this is
a render-time refusal. The KEDA ceiling counts because it is a reachable pod
count even when the StatefulSet starts at one replica.
*/}}
{{- define "siglake.queryJobsScaleGuard" -}}
{{- $max := int .Values.query.replicas -}}
{{- $from := "query.replicas" -}}
{{- if and .Values.keda.enabled .Values.keda.query.enabled -}}
{{- $kedaMax := int .Values.keda.query.maxReplicas -}}
{{- if gt $kedaMax $max -}}
{{- $max = $kedaMax -}}
{{- $from = "keda.query.maxReplicas" -}}
{{- end -}}
{{- end -}}
{{- if and .Values.query.enabled (gt $max 1) (not .Values.query.jobs.persistent) -}}
{{- fail (printf "%s=%d would run more than one query pod while query.jobs.persistent is false: each pod would keep a separate in-memory batch-job table, so status, result and cancel requests routed to another healthy pod would answer 404 job not found. Set query.jobs.persistent=true (the default), or hold the query tier at one pod (query.replicas=1 with keda.query.enabled=false, or keda.query.maxReplicas=1)." $from $max) -}}
{{- end -}}
{{- end -}}

{{/*
siglake.oidcGuard - FAIL THE INSTALL for a half-configured OIDC block on a tier
this render enables. Called with (list "<values path>" $issuer $audience
$tenantClaim), all three already trimmed by the caller, from inside the tier's
own `if .Values.<tier>.enabled` so a disabled tier's values stay inert.

Both tiers used to render the OIDC variables only under `if and .issuer
.audience`, which turned an incomplete block into an install with NO bearer
verification: an issuer with no audience, an audience with no issuer, or a
`tenantClaim` on its own disappeared during rendering, the pods came up on
whatever token list was left (or open), and nothing in the cluster said the
requested authentication was missing. The omission also skipped the binaries'
own fail-closed checks, which only see variables that were rendered.

The claim is the sharper half: it says tenancy comes from a verified token, so
without a verifier there is no token to take it from and the tier falls back to
the default namespace or, on the ingester with `trustScopeHeader`, to the
client's header. `siglake ingest-server` and `siglake-query-server` both refuse
that combination at startup (#4302); this refuses it before the release
installs.

Bearer tokens beside it change nothing: they authenticate a caller, they do not
verify the JWT the operator asked for.
*/}}
{{- define "siglake.oidcGuard" -}}
{{- $path := index . 0 -}}
{{- $issuer := index . 1 -}}
{{- $audience := index . 2 -}}
{{- $claim := index . 3 -}}
{{- if and $issuer (not $audience) -}}
{{- fail (printf "%s.oidc.issuer is set but %s.oidc.audience is empty: the chart renders the OIDC variables only as a pair, so this release would install a tier with no token verifier at all and accept whatever its remaining authentication allows. Set %s.oidc.audience, or clear %s.oidc.issuer." $path $path $path $path) -}}
{{- end -}}
{{- if and $audience (not $issuer) -}}
{{- fail (printf "%s.oidc.audience is set but %s.oidc.issuer is empty: the chart renders the OIDC variables only as a pair, so this release would install a tier with no token verifier at all and accept whatever its remaining authentication allows. Set %s.oidc.issuer, or clear %s.oidc.audience." $path $path $path $path) -}}
{{- end -}}
{{- if and $claim (not (and $issuer $audience)) -}}
{{- fail (printf "%s.oidc.tenantClaim is set but %s.oidc.issuer and %s.oidc.audience are empty: the tenant comes from a verified token, and with no OIDC verifier there is none to take it from, so every request would route by the tier's default instead. Set both, or clear %s.oidc.tenantClaim." $path $path $path $path) -}}
{{- end -}}
{{- end -}}

{{/*
siglake.oidcOn - "1" when a tier's OIDC block is COMPLETE, "" otherwise. Called
with (list $issuer $audience), both already trimmed by the caller. The pair is
the whole test: a tier renders SIGLAKE_OIDC_ISSUER and SIGLAKE_OIDC_AUDIENCE
only together, siglake.oidcGuard above refuses every other non-empty shape, and
the binaries build a verifier only from the pair. Anything that asks "does this
tier verify bearer tokens?" asks it here.
*/}}
{{- define "siglake.oidcOn" -}}
{{- $issuer := index . 0 -}}
{{- $audience := index . 1 -}}
{{- if and (ne $issuer "") (ne $audience "") -}}
1
{{- end -}}
{{- end -}}

{{/*
siglake.queryTokensSecret - the Secret name the query pods read
SIGLAKE_QUERY_TOKENS from, or "" when no allow-list reaches them at all.

#4273: three values can bring that Secret into existence and all three resolve
here. `query.tokens.existingSecret` names one the operator already has;
`query.tokens.list` makes the chart write one (secret-tokens.yaml); and
`externalSecrets.queryTokens.remoteKey` has External Secrets pull one
(externalsecret-query-tokens.yaml). The two chart-owned arms name the same
Secret, so they share one branch. Setting both of those while `existingSecret`
is empty is refused by the caller, which is the only place that knows the two
sources would fight over one name.
*/}}
{{- define "siglake.queryTokensSecret" -}}
{{- if .Values.query.tokens.existingSecret -}}
{{- .Values.query.tokens.existingSecret -}}
{{- else if or .Values.query.tokens.list (and .Values.externalSecrets.enabled .Values.externalSecrets.queryTokens.remoteKey) -}}
{{- include "siglake.componentName" (list . "query-tokens") -}}
{{- end -}}
{{- end -}}

{{/*
siglake.queryAuthOn - "1" when the query tier authenticates its callers, ""
when /api/v1/sql answers whoever can reach it. Two sources count, and either
one alone is authentication: a token allow-list from any of the three values
above, and a complete `query.oidc` block, which siglake-query-server selects
AHEAD of the token list.

#4317: the StatefulSet reads this to decide whether a fanned-out install needs
a coordinator token to present to its peers, and NOTES.txt to decide whether to
tell the operator the API is open. The two used to answer the question
separately, and an install that verified every caller's JWT still ended with a
note saying it had no authentication — and naming a token allow-list, the mode
OIDC takes precedence over, as the remedy.
*/}}
{{- define "siglake.queryAuthOn" -}}
{{- $oidc := default dict .Values.query.oidc -}}
{{- $issuer := trim (toString (default "" $oidc.issuer)) -}}
{{- $audience := trim (toString (default "" $oidc.audience)) -}}
{{- if or (include "siglake.queryTokensSecret" .) (include "siglake.oidcOn" (list $issuer $audience)) -}}
1
{{- end -}}
{{- end -}}

{{/*
siglake.ingestTokensSecret - the Secret name the ingester pods read
SIGLAKE_AUTH_TOKENS from, or "" when no static token allow-list reaches them.

`ingester.auth.existingSecret` names a Secret the operator already has;
`ingester.auth.list` makes the chart write one (secret-auth-tokens.yaml). Keep
the Deployment, chart-owned Secret and authentication note on this resolution
so a new token source cannot secure the pods while the note still calls them
open.
*/}}
{{- define "siglake.ingestTokensSecret" -}}
{{- if .Values.ingester.auth.existingSecret -}}
{{- .Values.ingester.auth.existingSecret -}}
{{- else if .Values.ingester.auth.list -}}
{{- include "siglake.componentName" (list . "auth-tokens") -}}
{{- end -}}
{{- end -}}

{{/*
siglake.ingestAuthOn - "1" when the ingest tier authenticates its callers, ""
when the OTLP and Elasticsearch-compatible write APIs answer whoever can reach
them. Either a static token allow-list or a complete `ingester.oidc` block is
authentication; the binary selects OIDC ahead of the static token list.
*/}}
{{- define "siglake.ingestAuthOn" -}}
{{- $oidc := default dict .Values.ingester.oidc -}}
{{- $issuer := trim (toString (default "" $oidc.issuer)) -}}
{{- $audience := trim (toString (default "" $oidc.audience)) -}}
{{- if or (include "siglake.ingestTokensSecret" .) (include "siglake.oidcOn" (list $issuer $audience)) -}}
1
{{- end -}}
{{- end -}}

{{- define "siglake.labels" -}}
helm.sh/chart: {{ include "siglake.chart" . }}
{{ include "siglake.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: siglake
{{- end -}}

{{- define "siglake.selectorLabels" -}}
app.kubernetes.io/name: {{ include "siglake.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "siglake.componentLabels" -}}
{{- $top := index . 0 -}}
{{- $role := index . 1 -}}
{{ include "siglake.labels" $top }}
app.kubernetes.io/component: {{ $role }}
{{- end -}}

{{- define "siglake.componentSelectorLabels" -}}
{{- $top := index . 0 -}}
{{- $role := index . 1 -}}
{{ include "siglake.selectorLabels" $top }}
app.kubernetes.io/component: {{ $role }}
{{- end -}}

{{- define "siglake.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "siglake.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "siglake.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/*
siglake.componentImage - like siglake.image, but a component may override the
repository/tag (blank fields inherit the global image.*). Lets one component
(e.g. query) roll to a new build independently of the rest of the cluster.
Args: (list $top $override) where $override is the component's `image` map.
*/}}
{{- define "siglake.componentImage" -}}
{{- $top := index . 0 -}}
{{- $o := default dict (index . 1) -}}
{{- $repo := default $top.Values.image.repository $o.repository -}}
{{- $tag := default (default $top.Chart.AppVersion $top.Values.image.tag) $o.tag -}}
{{- printf "%s:%s" $repo $tag -}}
{{- end -}}

{{/*
Common environment variables.
Order matters: PG* vars must come before SIGLAKE_CATALOG_URI so the
$(VAR) substitution resolves at pod-creation time.
*/}}
{{- define "siglake.commonEnv" -}}
- name: RUST_LOG
  value: {{ .Values.logLevel | quote }}
- name: PGHOST
  valueFrom:
    secretKeyRef:
      name: {{ required "postgres.existingSecret is required" .Values.postgres.existingSecret | quote }}
      key: {{ .Values.postgres.secretKeys.host | quote }}
- name: PGPORT
  valueFrom:
    secretKeyRef:
      name: {{ .Values.postgres.existingSecret | quote }}
      key: {{ .Values.postgres.secretKeys.port | quote }}
- name: PGUSER
  valueFrom:
    secretKeyRef:
      name: {{ .Values.postgres.existingSecret | quote }}
      key: {{ .Values.postgres.secretKeys.user | quote }}
- name: PGPASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.postgres.existingSecret | quote }}
      key: {{ .Values.postgres.secretKeys.password | quote }}
- name: PGDATABASE
  valueFrom:
    secretKeyRef:
      name: {{ .Values.postgres.existingSecret | quote }}
      key: {{ .Values.postgres.secretKeys.database | quote }}
- name: SIGLAKE_CATALOG_URI
  value: "postgres://$(PGUSER):$(PGPASSWORD)@$(PGHOST):$(PGPORT)/$(PGDATABASE)"
- name: SIGLAKE_WAREHOUSE_URL
  value: {{ printf "s3://%s/%s/" (required "s3.bucket is required" .Values.s3.bucket) (trimAll "/" .Values.s3.warehousePrefix) | quote }}
- name: AWS_REGION
  value: {{ required "s3.region is required" .Values.s3.region | quote }}
- name: SIGLAKE_TENANT_NAMESPACE
  value: {{ .Values.tenant.namespace | quote }}
{{- if .Values.s3.endpoint }}
- name: AWS_ENDPOINT_URL
  value: {{ .Values.s3.endpoint | quote }}
- name: AWS_S3_FORCE_PATH_STYLE
  value: "true"
{{- end }}
{{- with .Values.extraEnv }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
Pod scheduling block — nodeSelector, tolerations, affinity.

Called with: (list $top $componentValues $role). Anti-affinity wiring
honors `.Values.antiAffinity.enabled` and adds podAntiAffinity rules
that spread same-role replicas across the configured topologyKey.
Customer-supplied `$cv.affinity` overrides the chart-managed block
entirely (escape hatch for advanced topologies).
*/}}
{{- define "siglake.scheduling" -}}
{{- $top := index . 0 -}}
{{- $cv := index . 1 -}}
{{- $role := index . 2 -}}
{{- with $cv.nodeSelector }}
nodeSelector:
{{ toYaml . | indent 2 }}
{{- end }}
{{- with $cv.tolerations }}
tolerations:
{{ toYaml . | indent 2 }}
{{- end }}
{{- if $cv.affinity }}
affinity:
{{ toYaml $cv.affinity | indent 2 }}
{{- else if $top.Values.antiAffinity.enabled }}
affinity:
  podAntiAffinity:
{{- if eq $top.Values.antiAffinity.mode "required" }}
    requiredDuringSchedulingIgnoredDuringExecution:
      - topologyKey: {{ $top.Values.antiAffinity.topologyKey | quote }}
        labelSelector:
          matchLabels:
{{ include "siglake.componentSelectorLabels" (list $top $role) | indent 12 }}
{{- else }}
    preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          topologyKey: {{ $top.Values.antiAffinity.topologyKey | quote }}
          labelSelector:
            matchLabels:
{{ include "siglake.componentSelectorLabels" (list $top $role) | indent 14 }}
{{- end }}
{{- end }}
{{- end -}}

{{/*
WAL volume — either an existingClaim (customer-managed) or the
chart-managed PVC. Called with the top-level context.
*/}}
{{- define "siglake.walClaimName" -}}
{{- if .Values.wal.existingClaim -}}
{{- .Values.wal.existingClaim -}}
{{- else -}}
{{- include "siglake.componentName" (list . "wal") -}}
{{- end -}}
{{- end -}}
