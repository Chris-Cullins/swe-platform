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
{{- printf "%s:%s" .Values.environmentImage.repository (.Values.environmentImage.tag | default .Chart.AppVersion) -}}
{{- end }}
