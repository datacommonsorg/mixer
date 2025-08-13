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
        throw DataCommonsException("Failed to parse JSON response.");
    }

    return json;
}

nlohmann::json DataCommons::GetObservations(
    const std::vector<std::string>& select,
    const ObservationVariable& variable,
    const ObservationEntity& entity,
    const ObservationDate& date,
    const ObservationFilter& filter) {
    nlohmann::json body;
    body["select"] = select;

    nlohmann::json variable_json = nlohmann::json::object();
    if (!variable.dcids.empty()) {
        variable_json["dcids"] = variable.dcids;
    }
    if (!variable.expression.empty()) {
        variable_json["expression"] = variable.expression;
    }
    body["variable"] = variable_json;

    nlohmann::json entity_json = nlohmann::json::object();
    if (!entity.dcids.empty()) {
        entity_json["dcids"] = entity.dcids;
    }
    if (!entity.expression.empty()) {
        entity_json["expression"] = entity.expression;
    }
    body["entity"] = entity_json;

    std::visit([&](auto&& arg) {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, std::string>) {
            if (!arg.empty()) {
                body["date"] = arg;
            }
        } else if constexpr (std::is_same_v<T, std::vector<std::string>>) {
            body["date"] = arg;
        }
    }, date);

    if (!filter.facet_ids.empty() || !filter.domains.empty()) {
        nlohmann::json filter_json = nlohmann::json::object();
        if (!filter.facet_ids.empty()) {
            filter_json["facet_ids"] = filter.facet_ids;
        }
        if (!filter.domains.empty()) {
            filter_json["domains"] = filter.domains;
        }
        body["filter"] = filter_json;
    }

    std::string response = Post("/v2/observation", body.dump());
    if (response.empty()) {
        return {};
    }

    auto json = nlohmann::json::parse(response, nullptr, false);
    if (json.is_discarded()) {
        throw DataCommonsException("Failed to parse JSON response.");
    }

    return json;
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
        throw DataCommonsException("Failed to parse JSON response.");
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
        throw DataCommonsException("Failed to parse JSON response.");
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
        throw DataCommonsException("Error: " + std::to_string(r.status_code) + " - " + r.error.message + "\n" + r.text);
    }
}

} // namespace datacommons
