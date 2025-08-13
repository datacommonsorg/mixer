#include "DataCommons.h"
#include <iostream>

void TestGetPropertyValues(datacommons::DataCommons& dc) {
    std::cout << "--- Testing GetPropertyValues ---" << std::endl;
    std::vector<std::string> dcids = {"geoId/06", "geoId/08"};
    std::vector<std::string> properties = {"name", "typeOf"};
    auto result = dc.GetPropertyValues(dcids, "->", properties);
    std::cout << result.dump(2) << std::endl;
    std::cout << std::endl;
}

void TestGetObservations(datacommons::DataCommons& dc) {
    std::cout << "--- Testing GetObservations (Example 1 from Docs) ---" << std::endl;
    std::vector<std::string> select = {"variable", "entity"};
    datacommons::ObservationVariable variables;
    datacommons::ObservationEntity entities;
    entities.dcids = {"country/TGO"};
    datacommons::ObservationDate date = ""; // Empty to get all dates
    auto result = dc.GetObservations(select, variables, entities, date);
    std::cout << result.dump(2) << std::endl;
    std::cout << std::endl;
}

void TestResolve(datacommons::DataCommons& dc) {
    std::cout << "--- Testing Resolve ---" << std::endl;
    std::vector<std::string> nodes = {"California", "Mountain View"};
    std::string property = "<-description->dcid";
    auto result = dc.Resolve(nodes, property);
    for (const auto& [node, candidates] : result) {
        std::cout << "Node: " << node << std::endl;
        for (const auto& candidate : candidates) {
            std::cout << "  DCID: " << candidate.dcid << ", Type: " << candidate.dominant_type << std::endl;
        }
    }
    std::cout << std::endl;
}

void TestQuery(datacommons::DataCommons& dc) {
    std::cout << "--- Testing Query ---" << std::endl;
    std::string query = "SELECT ?name ?dcid WHERE { ?place typeOf State . ?place name ?name . ?place dcid ?dcid . } LIMIT 5";
    auto result = dc.Query(query);
    for (const auto& header : result.header) {
        std::cout << header << "\t\t";
    }
    std::cout << std::endl;
    for (const auto& row : result.rows) {
        for (const auto& header : result.header) {
            std::cout << row.at(header) << "\t";
        }
        std::cout << std::endl;
    }
    std::cout << std::endl;
}

int main() {
    try {
        datacommons::DataCommons dc;
        TestGetPropertyValues(dc);
        TestGetObservations(dc);
        TestResolve(dc);
        TestQuery(dc);
    } catch (const datacommons::DataCommonsException& e) {
        std::cerr << "Data Commons Error: " << e.what() << std::endl;
        return 1;
    } catch (const std::runtime_error& e) {
        std::cerr << "Runtime Error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
