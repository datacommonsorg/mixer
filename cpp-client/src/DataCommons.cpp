#include "DataCommons.h"
#include <cpr/cpr.h>
#include <nlohmann/json.hpp>
#include <iostream>

namespace datacommons {

DataCommons::DataCommons(const std::string& api_key) : api_key_(api_key) {}

std::map<std::string, std::vector<PropertyValue>> DataCommons::GetPropertyValues(const std::vector<std::string>& dcids, const std::string& prop) {
    nlohmann::json body = {
        {"nodes", dcids},
        {"property", "->" + prop}
    };

    std::string response = Post("/v2/node", body.dump());
    if (response.empty()) {
        return {};
    }

    auto json = nlohmann::json::parse(response, nullptr, false);
    if (json.is_discarded()) {
        std::cerr << "Failed to parse JSON response." << std::endl;
        return {};
    }

    std::map<std::string, std::vector<PropertyValue>> result;
    if (json.contains("data")) {
        for (auto const& [dcid, data] : json["data"].items()) {
            if (data.contains("arcs")) {
                std::string arc_prop = "name";
                if (data["arcs"].contains(arc_prop)) {
                    for (const auto& node : data["arcs"][arc_prop]["nodes"]) {
                        if (node.contains("provenanceId") && node.contains("value")) {
                            result[dcid].push_back({node["provenanceId"], node["value"]});
                        }
                    }
                }
            }
        }
    }

    return result;
}

std::map<std::string, std::map<std::string, std::vector<Observation>>> DataCommons::GetObservations(
    const std::vector<std::string>& variables,
    const std::vector<std::string>& entities,
    const std::string& date) {
    nlohmann::json body = {
        {"select", {"variable", "entity", "date", "value", "provenanceId"}},
        {"from", {
            {"variable", {{"dcids", variables}}},
            {"entity", {{"dcids", entities}}},
            {"date", date}
        }}
    };

    std::string response = Post("/v2/observation", body.dump());
    if (response.empty()) {
        return {};
    }

    auto json = nlohmann::json::parse(response, nullptr, false);
    if (json.is_discarded()) {
        std::cerr << "Failed to parse JSON response." << std::endl;
        return {};
    }

    std::map<std::string, std::map<std::string, std::vector<Observation>>> result;
    if (json.contains("byVariable")) {
        for (const auto& var_data : json["byVariable"]) {
            if (var_data.contains("variable") && var_data.contains("byEntity")) {
                std::string variable = var_data["variable"];
                for (const auto& entity_data : var_data["byEntity"]) {
                    if (entity_data.contains("entity") && entity_data.contains("observations")) {
                        std::string entity = entity_data["entity"];
                        for (const auto& obs : entity_data["observations"]) {
                            if (obs.contains("date") && obs.contains("value") && obs.contains("provenanceId")) {
                                result[variable][entity].push_back({
                                    obs["date"],
                                    obs["value"],
                                    obs["provenanceId"]
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    return result;
}

std::string DataCommons::Post(const std::string& endpoint, const std::string& body) {
    cpr::Url url = cpr::Url{base_url_ + endpoint};

    cpr::Session session;
    session.SetUrl(url);
    session.SetHeader({{"X-API-Key", api_key_}, {"Content-Type", "application/json"}});
    session.SetBody(body);

    cpr::Response r = session.Post();
    if (r.status_code == 200) {
        return r.text;
    } else {
        std::cerr << "Error: " << r.status_code << " - " << r.error.message << std::endl;
        std::cerr << r.text << std::endl;
        return "";
    }
}

} // namespace datacommons
