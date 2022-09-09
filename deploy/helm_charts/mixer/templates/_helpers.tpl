{{/*
Copyright 2022 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

This is a helper module that defines variables and partial templates
used throughout other templates.

This module does not generate any yaml outputs, as it is prefixed by
"_" in the file name.

For more on partial templates and _ files, see:
https://helm.sh/docs/chart_template_guide/named_templates/#partials-and-_-files

Example: selectorLabels

"mixer.selectorLabels" is a partial template defined in this module.

{{- define "mixer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "mixer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

deployment.yaml includes the snippet above using the snippet below.

{{- include "mixer.selectorLabels" $ | nindent 6 }}
*/}}

{{/*
Expand the name of the chart.
*/}}
{{- define "mixer.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "mixer.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "mixer.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "mixer.labels" -}}
helm.sh/chart: {{ include "mixer.chart" . }}
{{ include "mixer.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "mixer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "mixer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
