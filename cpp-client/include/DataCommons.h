#ifndef DATACOMMONS_H
#define DATACOMMONS_H

#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include <map>
#include <variant>

namespace datacommons {

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

class DataCommons {
public:
    DataCommons();
    DataCommons(const std::string& api_key);

    // V2 Endpoints
    nlohmann::json GetPropertyValues(
        const std::vector<std::string>& dcids,
        const std::string& prop_direction,
        const std::vector<std::string>& properties);
    std::map<std::string, std::map<std::string, std::vector<Observation>>> GetObservations(
        const ObservationVariable& variable,
        const ObservationEntity& entity,
        const ObservationDate& date);
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
