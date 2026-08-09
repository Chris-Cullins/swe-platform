{{- define "swe-platform.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "swe-platform.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name (include "swe-platform.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "swe-platform.controlPlaneFullname" -}}
{{- printf "%s-control-plane" (include "swe-platform.fullname" . | trunc 49 | trimSuffix "-") -}}
{{- end }}

{{- define "swe-platform.operatorMetricsFullname" -}}
{{- printf "%s-operator-metrics" (include "swe-platform.fullname" . | trunc 46 | trimSuffix "-") -}}
{{- end }}

{{- define "swe-platform.clusterFullname" -}}
{{- printf "%s-%s" (include "swe-platform.fullname" . | trunc 53 | trimSuffix "-") (sha256sum .Release.Namespace | trunc 8) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "swe-platform.controlPlaneClusterFullname" -}}
{{- printf "%s-control-plane" (include "swe-platform.clusterFullname" . | trunc 49 | trimSuffix "-") -}}
{{- end }}

{{- define "swe-platform.controlPlaneEnvIntentFullname" -}}
{{- printf "%s-%s-control-plane-env-intent" (include "swe-platform.fullname" . | trunc 29 | trimSuffix "-") (sha256sum .Release.Namespace | trunc 8) -}}
{{- end }}

{{- define "swe-platform.controlPlanePortalStatusFullname" -}}
{{- printf "%s-%s-control-plane-portal-status" (include "swe-platform.fullname" . | trunc 26 | trimSuffix "-") (sha256sum .Release.Namespace | trunc 8) -}}
{{- end }}

{{- define "swe-platform.operatorPortalStatusFullname" -}}
{{- printf "%s-%s-operator-status" (include "swe-platform.fullname" . | trunc 37 | trimSuffix "-") (sha256sum .Release.Namespace | trunc 8) -}}
{{- end }}

{{- define "swe-platform.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "swe-platform.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "swe-platform.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "swe-platform.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "swe-platform.environmentImage" -}}
{{- if .Values.environmentImage.digest -}}
{{- printf "%s@%s" .Values.environmentImage.repository .Values.environmentImage.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.environmentImage.repository (.Values.environmentImage.tag | default .Chart.AppVersion) -}}
{{- end -}}
{{- end }}

{{- define "swe-platform.tenancyMode" -}}
{{- $mode := required "tenancy.mode is required and must be scoped or trusted-admin" .Values.tenancy.mode -}}
{{- if not (has $mode (list "scoped" "trusted-admin")) -}}
{{- fail "tenancy.mode must be scoped or trusted-admin" -}}
{{- end -}}
{{- $mode -}}
{{- end }}
