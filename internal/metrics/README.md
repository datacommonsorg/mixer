# OpenTelemetry Metrics

This package contains:

- Functions for setting up and tearing down OpenTelemetry on the mixer server.
  Currently this only has configuration for metrics, but it can be expanded
  to traces or logging in the future.

- Functions for recording individual metrics. If the list of these grows longer,
  they can be organized into separate files and/or packages.

- YAML configs for running containers locally to debug new metrics. Run commands
  for local containers are at the top of each file.

  - `prometheus_local.yml` works with `--metrics_collector=prometheus` and
     includes a web UI that scrapes metrics from a local mixer.

  - `otlp_collector_local.yml` works with `--metrics_collector=otlp` and
     validates the export mode used on the deployed mixer.
