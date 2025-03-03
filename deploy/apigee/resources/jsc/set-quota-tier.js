const DC_ENFORCE_QUOTA_FLOW_VAR = "datacommons_enforce_quota";
const DC_QUOTA_TIER_FLOW_VAR = "datacommons_quota_tier";
const DC_QUOTA_REQUESTS_PER_INTERVAL_FLOW_VAR = "datacommons_quota_requests_per_interval";
const DC_QUOTA_INTERVAL_FLOW_VAR = "datacommons_quota_interval";
const DC_QUOTA_TIMEUNIT_FLOW_VAR = "datacommons_quota_timeunit";
const DC_QUOTA_IDENTIFIER_FLOW_VAR = "datacommons_quota_identifier";

const DC_VERIFY_API_KEY_POLICY_NAME = "verify-api-key-in-header";
const VERIFY_API_KEY_FLOW_VAR_PREFIX = "verifyapikey." + DC_VERIFY_API_KEY_POLICY_NAME;
const DEVELOPER_QUOTA_TIER_FLOW_VAR = VERIFY_API_KEY_FLOW_VAR_PREFIX + ".developer.datacommons_quota_tier";
const DEVLOPER_EMAIl_FLOW_VAR = VERIFY_API_KEY_FLOW_VAR_PREFIX + ".developer.email";
const API_KEY_FLOW_VAR = VERIFY_API_KEY_FLOW_VAR_PREFIX + ".client_id";
const TRIAL_API_KEY_QUOTA_TIER = "trial-api-key";
const DEFAULT_QUOTA_TIER = "default";
const UNLIMITED_QUOTA_TIER = "unlimited";
const ENFORCE_QUOTA_QUERY_PARAM = "enforce_quota";

function is_quota_enforcement_enabled() {
    // Skip enforcement if api key not found
    var api_key = context.getVariable(API_KEY_FLOW_VAR);
    if (!api_key || api_key.trim().length == 0) {
        return false;
    }
    var enforce_quota_param = context.proxyRequest.queryParams[ENFORCE_QUOTA_QUERY_PARAM];
    return enforce_quota_param != null && enforce_quota_param == 'true'

}

function is_trial_api_key() {
    var api_key = context.getVariable(API_KEY_FLOW_VAR);
    return api_key && api_key.trim() == properties.trial_api_key;
}

function get_quota_tier() {
    var quota_tier = DEFAULT_QUOTA_TIER;
    if (is_trial_api_key()) {
        quota_tier = TRIAL_API_KEY_QUOTA_TIER;
    } else {
        var developer_quota_tier = context.getVariable(DEVELOPER_QUOTA_TIER_FLOW_VAR);
        print("Developer quota tier=[" + developer_quota_tier + "]");
        if (developer_quota_tier && developer_quota_tier.trim().length > 0) {
            quota_tier = developer_quota_tier.trim();
        }
    }
    return quota_tier;
}

function enforce_quota() {
    if (!is_quota_enforcement_enabled()) {
        context.setVariable(DC_ENFORCE_QUOTA_FLOW_VAR, "false");
        return;
    }

    var quota_tier = get_quota_tier();
    print("Quota tier=[" + quota_tier + "]");
    var enforce_quota = "true";
    var request_per_interval = "";
    var quota_interval = "";
    var quota_timeunit = "";
    var quota_identifier = "";
    switch (quota_tier) {
        case UNLIMITED_QUOTA_TIER:
            enforce_quota = "false";
            quota_tier = UNLIMITED_QUOTA_TIER;
            break;
        case TRIAL_API_KEY_QUOTA_TIER:
            enforce_quota = "true";
            quota_tier = TRIAL_API_KEY_QUOTA_TIER;
            quota_identifier = context.getVariable("proxy.client.ip");
            request_per_interval = properties.trial_key_quota_requests_per_interval;
            quota_interval = properties.trial_key_quota_interval_minutes;
            quota_timeunit = "minute";
            break;
        default:
            enforce_quota = "true";
            quota_tier = DEFAULT_QUOTA_TIER;
            quota_identifier = context.getVariable(DEVLOPER_EMAIl_FLOW_VAR);
            request_per_interval = properties.default_quota_requests_per_interval;
            quota_interval = properties.default_quota_interval;
            quota_timeunit = properties.default_quota_timeunit;
            break;
    }
    context.setVariable(DC_ENFORCE_QUOTA_FLOW_VAR, enforce_quota);
    context.setVariable(DC_QUOTA_TIER_FLOW_VAR, quota_tier);
    if (enforce_quota == "true") {
        context.setVariable(DC_QUOTA_IDENTIFIER_FLOW_VAR, quota_identifier);
        context.setVariable(DC_QUOTA_REQUESTS_PER_INTERVAL_FLOW_VAR, request_per_interval);
        context.setVariable(DC_QUOTA_INTERVAL_FLOW_VAR, quota_interval);
        context.setVariable(DC_QUOTA_TIMEUNIT_FLOW_VAR, quota_timeunit);
    }
}

enforce_quota();      