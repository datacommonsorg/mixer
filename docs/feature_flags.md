# Mixer Feature Flags

Feature flags can have their live values updated quickly (<15 min) on a per-environment basis. They can be used for gradual feature rollouts or for enabling and disabling features in each environment without waiting for a new binary release.

## [Code Locations](#code-locations)

- datacommonsorg/mixer repo:
  - [deploy/featureflags](https://github.com/datacommonsorg/mixer/tree/master/deploy/featureflags)
    - Per-environment config YAMLs
    - Script for deploying flags to one or all environments
    - Script for installing or upgrading Reloader
  - [scripts/check_flags.sh](https://github.com/datacommonsorg/mixer/blob/master/scripts/check_flags.sh) and [scripts/check_flags.go](https://github.com/datacommonsorg/mixer/blob/master/scripts/check_flags.go)
  - [internal/featureflags/featureflags.go](https://github.com/datacommonsorg/mixer/blob/master/internal/featureflags/featureflags.go)
  - [build/ci/cloudbuild.deploy_flags.yaml](https://github.com/datacommonsorg/mixer/blob/master/build/ci/cloudbuild.deploy_flags.yaml)
  - [.github/workflows/feature-flag-checks.yml](https://github.com/datacommonsorg/mixer/blob/master/.github/workflows/feature-flag-checks.yml)
- datcom-ci GCP project
  - [mixer-deploy-feature-flags](https://pantheon.corp.google.com/cloud-build/triggers?project=datcom-ci&e=13803378&mods=-monitoring_api_staging&pageState=(%22triggers%22:(%22f%22:%22%255B%257B_22k_22_3A_22Name_22_2C_22t_22_3A10_2C_22v_22_3A_22_5C_22mixer-deploy-feature-flags_~*mixer-deploy-feature-flags_5C_22_22_2C_22s_22_3Atrue_2C_22i_22_3A_22name_22%257D%255D%22))) trigger
  - [Trigger failure alert policy](https://pantheon.corp.google.com/monitoring/alerting/policies/2406337156848324980?e=13803378&mods=-monitoring_api_staging&project=datcom-ci) (sends an email to datacommons-alerts@ with the subject "Mixer feature flags failed to deploy")

## [Infrastructure](#infra)

- **Config YAML files**: The mixer server reads feature flag values from a config YAML file available as a ConfigMap **not** managed by Helm.
  - The config file path is passed to the mixer server as a command line flag.
  - The config YAML also lists the GKE clusters that the ConfigMap will be pushed to
- **Environments**: Separate environments are defined for each release stage and for standalone mixer vs mixer bundled with the website. In other words, there is a `prod.yaml` for standalone prod mixer and a `prod_website.yaml` for the mixer used by the production website.
- **Cloud Build trigger**: When changes to an environment's YAML file are merged into the mixer repo master branch, a Cloud Build trigger syncs the ConfigMap contents in each environment to the branch head.
- **GKE rolling restarts via Reloader**: ConfigMap changes are only picked up by GKE pods after they restart, so a tool called Reloader watches for changes to the ConfigMap contents and performs a rolling restart of mixer pods.
  - The Helm chart has a tag that tells Reloader which ConfigMap changes to care about.
  - The pods only restart if the ConfigMap contents have changed.
    - Use the ["Logs" tab on the reloader-reloader GKE Deployment page](https://screenshot.googleplex.com/82JhPTATrdK5yTf) to see when changes were detected. Note that one log line is printed for each mixer service group, so one full rolling restart = ~5 log lines.
  - To turn up Reloader on a new cluster or update an existing deployment, use `install_reloader.sh`
- **Default values**: Each flag has a default value that is used when no value is read from config YAML.
- **Config <-> binary compatibility checks**: A `check_flags` utility runs before deployment to make sure the config YAML is compatible with the server binary before attempting to sync values and restart pods. If the config is incompatible, the pods will fail to start.
  - The commit to check against is pulled from the /version endpoint on the `liveUrl` listed in the environment's config YAML.
  - Compatibility issues arise when a flag value in config cannot be parsed as the expected type in the binary—for example, if a float-valued flag has a string value defined.
  - A flag value may also be incompatible if it fails manually-added validation checks in the featureflags package.
  - It is not a problem if a flag is defined in the binary but not in the config, or vice versa (in the former case, the flag will fall back to default value).
- **Time/date prod change restrictions**: A GitHub check `enforce_feature_flag_merge_restrictions` disallows merging prod flag changes on weekends, on Fridays, or outside of California business hours.
  - This check can be bypassed by running the `deploy_flags.sh` script manually: `deploy/featureflags/deploy_flags.sh deploy/featureflags prod`

## [How to check current live flag values](#check)

Go to the /version endpoint. This works for standalone mixer as well as website.

If a flag is not listed, it is not yet defined in code in that environment.

## [How to add a flag](#add)

Add new flags to [featureflags.go](https://github.com/datacommonsorg/mixer/blob/master/internal/featureflags/featureflags.go).

- Be sure to add a default value. This value will be used as a fallback if no value for the flag is found in config.
- Add any custom validation (e.g. range restriction for a float-valued flag representing a rollout fraction) to `validateFlagValues`.

If your flag will be used for a gradual rollout, define a float-valued flag and compare it to a random value to decide whether to enable functionality.

Note: Flags should only be used for temporary values, e.g. for gradual feature rollout or for testing a feature before enabling it fully. Long-running feature toggles should be maintained as regular command line flags.

## [How to update a live flag value](#update)

1. Edit the config YAML for the target environment
  - Note that the values for standalone mixer and mixer bundled with website must be changed separately
1. Merge the changes into the mixer main branch with a reviewed PR
  - Prod changes can only be merged on Monday through Thursday between 8:00 AM and 5:00 PM in California.
1. [Optional] If your feature change will cause APIs to return different data:
  - Clear the Redis cache: from the **website** repo, `./tools/clearcache/run.sh mixer <env>`
  - Update goldens: from the website repo, `gcloud builds submit --config build/ci/cloudbuild.update_nl_goldens.yaml --project=datcom-ci`

## [How to debug flag value rollout](#debug)

- Look at the logs for the Reloader workload in GKE for the environment you are curious about (e.g. for mixer prod, [GKE workloads UI for project datcom-mixer](https://pantheon.corp.google.com/kubernetes/workload/overview?project=datcom-mixer)). Look for lines mentioning "Changes detected".
- View the contents of the `mixer-feature-flag` ConfigMap directly in the GKE UI
- Look at [recent runs of the mixer-deploy-feature-flags trigger](https://pantheon.corp.google.com/cloud-build/builds;region=global?query=trigger_id%3D%222fb11dfa-1531-4e65-ba88-ef9e74525216%22&e=13803378&mods=-monitoring_api_staging&project=datcom-ci)

### [Manual updates](#manual-updates)

To re-deploy flags from the master branch, you can manually run the `mixer-deploy-feature-flags` trigger in Cloud Build, optionally providing an `_ENV` variable (e.g. `staging_website`).

To deploy flags from your local copy of the mixer repo, you can run the `deploy_flags.sh` script. This will bypass time-of-day checks, but still run binary compatibility checks.

If something goes terribly wrong, you could also delete the `mixer-feature-flags` ConfigMap from a GKE cluster manually. This should trigger Reloader restart mixer pods, at which point they will revert to default flag values.
