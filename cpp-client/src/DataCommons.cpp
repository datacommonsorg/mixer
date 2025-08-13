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

nlohmann::json DataCommons::GetPropertyValues(
    const std::vector<std::string>& dcids,
    const std::string& prop_direction,
    const std::vector<std::string>& properties) {
    std::stringstream ss;
    ss << prop_direction;
    if (properties.size() > 1) {
        ss << "[";
    }
    for (size_t i = 0; i < properties.size(); ++i) {
        if (i != 0) {
            ss << ",";
        }
        ss << properties[i];
    }
    if (properties.size() > 1) {
        ss << "]";
    }

    nlohmann::json body = {
        {"nodes", dcids},
        {"property", ss.str()}
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

    return json;
}

std::map<std::string, std::map<std::string, std::vector<Observation>>> DataCommons::GetObservations(
    const ObservationVariable& variable,
    const ObservationEntity& entity,
    const ObservationDate& date) {
    nlohmann::json body;
    body["select"] = {"variable", "entity", "date", "value", "provenanceId"};

    nlohmann::json from;
    if (!variable.dcids.empty()) {
        from["variable"]["dcids"] = variable.dcids;
    }
    if (!variable.expression.empty()) {
        from["variable"]["expression"] = variable.expression;
    }
    if (!entity.dcids.empty()) {
        from["entity"]["dcids"] = entity.dcids;
    }
    if (!entity.expression.empty()) {
        from["entity"]["expression"] = entity.expression;
    }

    std::visit([&](auto&& arg) {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, std::string>) {
            from["date"] = arg;
        } else if constexpr (std::is_same_v<T, std::vector<std::string>>) {
            from["date"] = arg;
        }
    }, date);

    body["from"] = from;

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
    const std::string& property) {
    nlohmann::json body = {
        {"nodes", nodes},
        {"property", property}
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
