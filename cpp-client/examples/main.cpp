#include "DataCommons.h"
#include <iostream>
#include <stdexcept>

int main() {
    try {
        // The DataCommons client will automatically look for the DC_API_KEY
        // environment variable.
        datacommons::DataCommons dc;

        // Resolve example
        std::vector<std::string> nodes = {"California", "Colorado"};
        std::string property = "<-description->dcid";
        auto resolve_result = dc.Resolve(nodes, property);
        for (const auto& [node, candidates] : resolve_result) {
            std::cout << "Node: " << node << std::endl;
            for (const auto& candidate : candidates) {
                std::cout << "  DCID: " << candidate.dcid << ", Type: " << candidate.dominant_type << std::endl;
            }
        }
    } catch (const std::runtime_error& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
