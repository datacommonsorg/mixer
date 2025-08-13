#include "DataCommons.h"
#include <iostream>
#include <stdexcept>

int main() {
    try {
        // The DataCommons client will automatically look for the DC_API_KEY
        // environment variable.
        datacommons::DataCommons dc;

        // GetPropertyValues example
        std::vector<std::string> dcids = {"geoId/06", "geoId/08"};
        std::string prop = "name";
        auto result = dc.GetPropertyValues(dcids, prop);
        for (const auto& [dcid, values] : result) {
            std::cout << "DCID: " << dcid << std::endl;
            for (const auto& value : values) {
                std::cout << "  Value: " << value.value << std::endl;
            }
        }
    } catch (const std::runtime_error& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
