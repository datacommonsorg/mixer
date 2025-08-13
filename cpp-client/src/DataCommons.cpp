#include "DataCommons.h"
#include <cpr/cpr.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <stdexcept>
#include <cstdlib>
#include <sstream>

namespace datacommons {

DataCommons::DataCommons() {
    const char* api_key_env = std::getenv("DC_API_KEY");
    if (api_key_env == nullptr || std::string(api_key_env).empty()) {
        throw std::runtime_error("API key not found. Please set the DC_API_KEY environment variable.");
    }
    api_key_ = api_key_env;
}

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

std::map<std::string, std::vector<ResolvedId>> DataCommons::Resolve(
    const std::vector<std::string>& nodes,
    const std::string& from_property,
    const std::string& to_property) {
    std::string expression = "<-" + from_property + "->" + to_property;
    nlohmann::json body = {
        {"nodes", nodes},
        {"property", expression}
    };

    std::string response = Post("/v2/resolve", body.dump());
    if (response.empty()) {
        return {};
    }

    auto json = nlohmann::json::parse(response, nullptr, false);
    if (json.is_discarded()) {
        std::cerr << "Failed to parse JSON response." << std::endl;
        return {};
    }

    std::map<std::string, std::vector<ResolvedId>> result;
    if (json.contains("entities")) {
        for (const auto& entity : json["entities"]) {
            if (entity.contains("node") && entity.contains("candidates")) {
                std::string node = entity["node"];
                for (const auto& candidate : entity["candidates"]) {
                    if (candidate.contains("dcid")) {
                        result[node].push_back({
                            candidate["dcid"],
                            candidate.value("dominantType", "")
                        });
                    }
                }
            }
        }
    }

    return result;
}

QueryResult DataCommons::Query(const std::string& query) {
    nlohmann::json body = {
        {"query", query}
    };

    std::string response = Post("/v2/sparql", body.dump());
    if (response.empty()) {
        return {};
    }

    auto json = nlohmann::json::parse(response, nullptr, false);
    if (json.is_discarded()) {
        std::cerr << "Failed to parse JSON response." << std::endl;
        return {};
    }

    QueryResult result;
    if (json.contains("header")) {
        for (const auto& header : json["header"]) {
            result.header.push_back(header);
        }
    }
    if (json.contains("rows")) {
        for (const auto& row : json["rows"]) {
            std::map<std::string, std::string> row_map;
            for (size_t i = 0; i < result.header.size(); ++i) {
                if (row.contains("cells") && i < row["cells"].size() && row["cells"][i].contains("value")) {
                    row_map[result.header[i]] = row["cells"][i]["value"];
                }
            }
            result.rows.push_back(row_map);
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
