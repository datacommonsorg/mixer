#ifndef DATACOMMONS_H
#define DATACOMMONS_H

#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include <map>
#include <variant>
#include <stdexcept>

namespace datacommons {

class DataCommonsException : public std::runtime_error {
public:
    DataCommonsException(const std::string& message) : std::runtime_error(message) {}
};

struct Observation {
    std::string date;
    double value;
    std::string provenance_id;
};

struct ResolvedId {
    std::string dcid;
    std::string dominant_type;
};

struct QueryResult {
    std::vector<std::string> header;
    std::vector<std::map<std::string, std::string>> rows;
};

struct ObservationVariable {
    std::vector<std::string> dcids;
    std::string expression;
};

struct ObservationEntity {
    std::vector<std::string> dcids;
    std::string expression;
};

using ObservationDate = std::variant<std::string, std::vector<std::string>>;

struct ObservationFilter {
    std::vector<std::string> facet_ids;
    std::vector<std::string> domains;
};

class DataCommons {
public:
    DataCommons();
    DataCommons(const std::string& api_key);

    // V2 Endpoints
    nlohmann::json GetPropertyValues(
        const std::vector<std::string>& dcids,
        const std::string& prop_direction,
        const std::vector<std::string>& properties);
    nlohmann::json GetObservations(
        const std::vector<std::string>& select,
        const ObservationVariable& variable,
        const ObservationEntity& entity,
        const ObservationDate& date,
        const ObservationFilter& filter = {});
    std::map<std::string, std::vector<ResolvedId>> Resolve(
        const std::vector<std::string>& nodes,
        const std::string& property);
    QueryResult Query(const std::string& query);

private:
    std::string api_key_;
    std::string base_url_ = "https://api.datacommons.org";

    std::string Post(const std::string& endpoint, const std::string& body);
};

} // namespace datacommons

#endif // DATACOMMONS_H
