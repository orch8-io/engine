{{/* Chart name. */}}
{{- define "orch8.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully qualified app name. */}}
{{- define "orch8.fullname" -}}
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

{{- define "orch8.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "orch8.labels" -}}
helm.sh/chart: {{ include "orch8.chart" . }}
{{ include "orch8.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "orch8.selectorLabels" -}}
app.kubernetes.io/name: {{ include "orch8.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "orch8.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}

{{- define "orch8.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "orch8.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* Secret holding api key / encryption key (and inline DB URL). */}}
{{- define "orch8.secretName" -}}
{{- default (include "orch8.fullname" .) .Values.secrets.existingSecret -}}
{{- end -}}

{{/* Name of the component that serves the HTTP API + /metrics. */}}
{{- define "orch8.apiComponent" -}}
{{- if eq .Values.mode "split" -}}control{{- else -}}all-in-one{{- end -}}
{{- end -}}

{{- define "orch8.apiServiceName" -}}
{{- printf "%s-%s" (include "orch8.fullname" .) (include "orch8.apiComponent" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "orch8.metricsJobName" -}}
{{- default (include "orch8.apiServiceName" .) .Values.metrics.jobName -}}
{{- end -}}

{{- define "orch8.postgresHost" -}}
{{- printf "%s-postgresql" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Cross-field validation. Rendered from every template that needs it; `fail`
aborts install/template with an actionable message.
*/}}
{{- define "orch8.validate" -}}
{{- $v := .Values -}}
{{- if eq $v.storage.backend "sqlite" -}}
  {{- if ne $v.mode "allInOne" -}}
    {{- fail "storage.backend=sqlite requires mode=allInOne: SQLite supports a single writer node only; use Postgres for split roles." -}}
  {{- end -}}
  {{- if ne (int $v.allInOne.replicas) 1 -}}
    {{- fail "storage.backend=sqlite requires allInOne.replicas=1 (single writer). Use Postgres for multiple replicas." -}}
  {{- end -}}
  {{- if $v.allInOne.autoscaling.enabled -}}
    {{- fail "storage.backend=sqlite cannot be combined with allInOne.autoscaling.enabled." -}}
  {{- end -}}
  {{- if $v.gateway.enabled -}}
    {{- fail "gateway.enabled requires storage.backend=postgres (a second process cannot share the SQLite file)." -}}
  {{- end -}}
  {{- if $v.postgresql.enabled -}}
    {{- fail "postgresql.enabled=true conflicts with storage.backend=sqlite." -}}
  {{- end -}}
{{- else -}}
  {{- if and (not $v.postgresql.enabled) (not $v.externalDatabase.url) (not $v.externalDatabase.existingSecret) -}}
    {{- fail "storage.backend=postgres needs externalDatabase.url, externalDatabase.existingSecret, or postgresql.enabled=true." -}}
  {{- end -}}
{{- end -}}
{{- if and (not $v.secrets.existingSecret) $v.secrets.encryptionKey -}}
  {{- if not (regexMatch "^[0-9a-fA-F]{64}$" $v.secrets.encryptionKey) -}}
    {{- fail "secrets.encryptionKey must be exactly 64 hex characters (openssl rand -hex 32)." -}}
  {{- end -}}
{{- end -}}
{{- if and $v.gateway.enabled (not $v.gateway.tls.existingSecret) -}}
  {{- fail "gateway.enabled requires gateway.tls.existingSecret with the gRPC server cert, key and client CA." -}}
{{- end -}}
{{- if and $v.gateway.enabled (not $v.config.requireTenantHeader) -}}
  {{- fail "gateway.enabled requires config.requireTenantHeader=true." -}}
{{- end -}}
{{- end -}}

{{/* Environment shared by every engine container. Arg: dict root role httpAddr. */}}
{{- define "orch8.env" -}}
{{- $root := .root -}}
{{- $v := $root.Values -}}
- name: ORCH8_NODE_ROLE
  value: {{ .role | quote }}
- name: ORCH8_HTTP_ADDR
  value: {{ .httpAddr | quote }}
- name: ORCH8_GRPC_ADDR
  value: {{ printf "0.0.0.0:%v" $v.service.grpcPort | quote }}
- name: ORCH8_STORAGE_BACKEND
  value: {{ $v.storage.backend | quote }}
{{- if eq $v.storage.backend "sqlite" }}
- name: ORCH8_DATABASE_URL
  value: "sqlite:///data/orch8.db?mode=rwc"
{{- else if $v.postgresql.enabled }}
- name: ORCH8_PG_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "orch8.postgresHost" $root }}
      key: password
- name: ORCH8_DATABASE_URL
  value: {{ printf "postgres://%s:$(ORCH8_PG_PASSWORD)@%s:5432/%s" $v.postgresql.auth.username (include "orch8.postgresHost" $root) $v.postgresql.auth.database | quote }}
{{- else if $v.externalDatabase.existingSecret }}
- name: ORCH8_DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ $v.externalDatabase.existingSecret }}
      key: {{ $v.externalDatabase.existingSecretKey }}
{{- else }}
- name: ORCH8_DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "orch8.fullname" $root }}
      key: database-url
{{- end }}
- name: ORCH8_RUN_MIGRATIONS
  value: {{ ternary "true" "false" (and (eq $v.storage.backend "postgres") (eq $v.migrations.mode "server")) | quote }}
- name: ORCH8_API_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "orch8.secretName" $root }}
      key: {{ $v.secrets.apiKeyKey }}
- name: ORCH8_ENCRYPTION_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "orch8.secretName" $root }}
      key: {{ $v.secrets.encryptionKeyKey }}
- name: ORCH8_REQUIRE_TENANT_HEADER
  value: {{ $v.config.requireTenantHeader | toString | quote }}
- name: ORCH8_LOG_LEVEL
  value: {{ $v.config.logLevel | quote }}
- name: ORCH8_LOG_JSON
  value: {{ $v.config.logJson | toString | quote }}
{{- if $v.config.corsOrigins }}
- name: ORCH8_CORS_ORIGINS
  value: {{ $v.config.corsOrigins | quote }}
{{- end }}
{{- with $v.extraEnv }}
{{ toYaml . }}
{{- end }}
{{- with .extraEnv }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
Engine Deployment. Arg: dict
  root, component (all-in-one|control|executor), role, cfg (per-role values).
*/}}
{{- define "orch8.deployment" -}}
{{- $root := .root -}}
{{- $v := $root.Values -}}
{{- $cfg := .cfg -}}
{{- $sqlite := eq $v.storage.backend "sqlite" -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "orch8.fullname" $root }}-{{ .component }}
  labels:
    {{- include "orch8.labels" $root | nindent 4 }}
    app.kubernetes.io/component: {{ .component }}
spec:
  {{- if not $cfg.autoscaling.enabled }}
  replicas: {{ $cfg.replicas }}
  {{- end }}
  {{- if $sqlite }}
  strategy:
    type: Recreate
  {{- else }}
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 1
      maxSurge: 1
  {{- end }}
  selector:
    matchLabels:
      {{- include "orch8.selectorLabels" $root | nindent 6 }}
      app.kubernetes.io/component: {{ .component }}
  template:
    metadata:
      labels:
        {{- include "orch8.selectorLabels" $root | nindent 8 }}
        app.kubernetes.io/component: {{ .component }}
      annotations:
        checksum/config: {{ $v.config.toml | sha256sum }}
        {{- with $cfg.podAnnotations }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
    spec:
      serviceAccountName: {{ include "orch8.serviceAccountName" $root }}
      automountServiceAccountToken: {{ $v.serviceAccount.automountServiceAccountToken }}
      {{- with $v.imagePullSecrets }}
      imagePullSecrets:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      terminationGracePeriodSeconds: {{ $v.terminationGracePeriodSeconds }}
      securityContext:
        {{- toYaml $v.podSecurityContext | nindent 8 }}
      containers:
        - name: orch8
          image: {{ include "orch8.image" $root }}
          imagePullPolicy: {{ $v.image.pullPolicy }}
          {{- if $v.config.toml }}
          args: ["--config", "/etc/orch8/orch8.toml"]
          {{- end }}
          securityContext:
            {{- toYaml $v.securityContext | nindent 12 }}
          ports:
            - name: http
              containerPort: 8080
              protocol: TCP
            - name: grpc
              containerPort: {{ $v.service.grpcPort }}
              protocol: TCP
          env:
            {{- include "orch8.env" (dict "root" $root "role" .role "httpAddr" "0.0.0.0:8080" "extraEnv" $cfg.extraEnv) | nindent 12 }}
          startupProbe:
            httpGet: { path: /health/live, port: http }
            periodSeconds: {{ $v.probes.startup.periodSeconds }}
            failureThreshold: {{ $v.probes.startup.failureThreshold }}
          readinessProbe:
            httpGet: { path: /health/ready, port: http }
            periodSeconds: {{ $v.probes.readiness.periodSeconds }}
            timeoutSeconds: {{ $v.probes.readiness.timeoutSeconds }}
            failureThreshold: {{ $v.probes.readiness.failureThreshold }}
          livenessProbe:
            httpGet: { path: /health/live, port: http }
            periodSeconds: {{ $v.probes.liveness.periodSeconds }}
            timeoutSeconds: {{ $v.probes.liveness.timeoutSeconds }}
            failureThreshold: {{ $v.probes.liveness.failureThreshold }}
          resources:
            {{- toYaml $cfg.resources | nindent 12 }}
          volumeMounts:
            - name: tmp
              mountPath: /tmp
            {{- if $sqlite }}
            - name: data
              mountPath: /data
            {{- end }}
            {{- if $v.config.toml }}
            - name: config
              mountPath: /etc/orch8
              readOnly: true
            {{- end }}
      volumes:
        - name: tmp
          emptyDir: {}
        {{- if $sqlite }}
        - name: data
          persistentVolumeClaim:
            claimName: {{ default (printf "%s-data" (include "orch8.fullname" $root)) $v.sqlite.persistence.existingClaim }}
        {{- end }}
        {{- if $v.config.toml }}
        - name: config
          configMap:
            name: {{ include "orch8.fullname" $root }}-config
        {{- end }}
      {{- with $cfg.nodeSelector }}
      nodeSelector:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with $cfg.tolerations }}
      tolerations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with $cfg.affinity }}
      affinity:
        {{- toYaml . | nindent 8 }}
      {{- end }}
{{- end -}}

{{/* HPA. Arg: dict root component cfg */}}
{{- define "orch8.hpa" -}}
{{- if .cfg.autoscaling.enabled }}
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {{ include "orch8.fullname" .root }}-{{ .component }}
  labels:
    {{- include "orch8.labels" .root | nindent 4 }}
    app.kubernetes.io/component: {{ .component }}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: {{ include "orch8.fullname" .root }}-{{ .component }}
  minReplicas: {{ .cfg.autoscaling.minReplicas }}
  maxReplicas: {{ .cfg.autoscaling.maxReplicas }}
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: {{ .cfg.autoscaling.targetCPUUtilizationPercentage }}
{{- end }}
{{- end -}}

{{/* PDB. Arg: dict root component cfg */}}
{{- define "orch8.pdb" -}}
{{- if .cfg.pdb.enabled }}
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: {{ include "orch8.fullname" .root }}-{{ .component }}
  labels:
    {{- include "orch8.labels" .root | nindent 4 }}
    app.kubernetes.io/component: {{ .component }}
spec:
  maxUnavailable: {{ .cfg.pdb.maxUnavailable }}
  selector:
    matchLabels:
      {{- include "orch8.selectorLabels" .root | nindent 6 }}
      app.kubernetes.io/component: {{ .component }}
{{- end }}
{{- end -}}

{{/* Service. Arg: dict root component http(bool) metrics(bool) */}}
{{- define "orch8.service" -}}
{{- $v := .root.Values -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ include "orch8.fullname" .root }}-{{ .component }}
  labels:
    {{- include "orch8.labels" .root | nindent 4 }}
    app.kubernetes.io/component: {{ .component }}
    {{- if .metrics }}
    orch8.io/metrics-job: {{ include "orch8.metricsJobName" .root }}
    {{- end }}
  {{- with $v.service.annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  type: {{ $v.service.type }}
  selector:
    {{- include "orch8.selectorLabels" .root | nindent 4 }}
    app.kubernetes.io/component: {{ .component }}
  ports:
    {{- if .http }}
    - name: http
      port: {{ $v.service.httpPort }}
      targetPort: http
      protocol: TCP
    {{- end }}
    - name: grpc
      port: {{ $v.service.grpcPort }}
      targetPort: grpc
      protocol: TCP
{{- end -}}
