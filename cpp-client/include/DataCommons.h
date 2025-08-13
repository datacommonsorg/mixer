#ifndef DATACOMMONS_H
#define DATACOMMONS_H

#include <string>
#include <vector>
#include <map>

namespace datacommons {

struct PropertyValue {
    std::string dcid;
    std::string value;
};

struct Observation {
    std::string date;
    double value;
    std::string provenance_id;
};

class DataCommons {
public:
    DataCommons(const std::string& api_key);

    // V2 Endpoints
    std::map<std::string, std::vector<PropertyValue>> GetPropertyValues(const std::vector<std::string>& dcids, const std::string& prop);
    std::map<std::string, std::map<std::string, std::vector<Observation>>> GetObservations(
        const std::vector<std::string>& variables,
        const std::vector<std::string>& entities,
        const std::string& date);

private:
    std::string api_key_;
    std::string base_url_ = "https://api.datacommons.org";

    std::string Post(const std::string& endpoint, const std::string& body);
};

} // namespace datacommons

#endif // DATACOMMONS_H
