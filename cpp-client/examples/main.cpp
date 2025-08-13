#include "DataCommons.h"
#include <iostream>

int main() {
    // NOTE: Please provide a valid API key.
    datacommons::DataCommons dc("AIzaSyCTI4Xz-UW_G2Q2RfknhcfdAnTHq5X5XuI");

    // GetPropertyValues example
    // std::vector<std::string> dcids = {"geoId/06", "geoId/08"};
    // std::string prop = "name";
    // auto result = dc.GetPropertyValues(dcids, prop);
    // for (const auto& [dcid, values] : result) {
    //     std::cout << "DCID: " << dcid << std::endl;
    //     for (const auto& value : values) {
    //         std::cout << "  Value: " << value.value << std::endl;
    //     }
    // }

    // GetObservations example
    std::vector<std::string> variables = {"Count_Person", "Count_Person_Male", "Count_Person_Female"};
    std::vector<std::string> entities = {"geoId/06", "geoId/08"};
    std::string date = "2020";
    auto obs_result = dc.GetObservations(variables, entities, date);
    for (const auto& [variable, entity_map] : obs_result) {
        std::cout << "Variable: " << variable << std::endl;
        for (const auto& [entity, observations] : entity_map) {
            std::cout << "  Entity: " << entity << std::endl;
            for (const auto& obs : observations) {
                std::cout << "    Date: " << obs.date << ", Value: " << obs.value << std::endl;
            }
        }
    }

    return 0;
}
