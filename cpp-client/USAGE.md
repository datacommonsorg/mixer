# Data Commons C++ Client Usage Guide

This guide provides a summary of the available endpoints in the Data Commons C++ client library and examples of how to use them.

## Getting Started

First, ensure you have set your Data Commons API key as an environment variable:

```bash
export DC_API_KEY="YOUR_API_KEY"
```

Then, you can create a `DataCommons` client object in your C++ code:

```cpp
#include "DataCommons.h"
#include <iostream>
#include <stdexcept>

int main() {
    try {
        datacommons::DataCommons dc;
        // Your code here...
    } catch (const std::runtime_error& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
```

## Core V2 API Endpoints

The C++ client provides access to the four core V2 endpoints of the Data Commons REST API.

### 1. GetPropertyValues

Fetches property values for one or more nodes. This method returns the raw JSON response from the API, giving you the flexibility to parse it as needed.

**Use Case:** Find the name and type of a place, like a state or city.

**Example:** Get the `name` and `typeOf` for California (`geoId/06`) and Colorado (`geoId/08`).

```cpp
std::vector<std::string> dcids = {"geoId/06", "geoId/08"};
std::vector<std::string> properties = {"name", "typeOf"};
auto result = dc.GetPropertyValues(dcids, "->", properties);

std::cout << result.dump(2) << std::endl;
```

### 2. GetObservations

Fetches statistical observations. This endpoint provides a flexible way to query for data by specifying variables, entities, and dates in various combinations.

**Use Case:** Get the total, male, and female population counts for California and Colorado in the year 2020.

**Example:**

```cpp
datacommons::ObservationVariable variables;
variables.dcids = {"Count_Person", "Count_Person_Male", "Count_Person_Female"};

datacommons::ObservationEntity entities;
entities.dcids = {"geoId/06", "geoId/08"};

datacommons::ObservationDate date = "2020";

auto result = dc.GetObservations(variables, entities, date);

for (const auto& [variable, entity_map] : result) {
    std::cout << "Variable: " << variable << std::endl;
    for (const auto& [entity, observations] : entity_map) {
        std::cout << "  Entity: " << entity << std::endl;
        for (const auto& obs : observations) {
            std::cout << "    Date: " << obs.date << ", Value: " << obs.value << std::endl;
        }
    }
}
```

You can also use expressions to select entities, for example, to get the population of all counties in California:

```cpp
datacommons::ObservationVariable variables;
variables.dcids = {"Count_Person"};

datacommons::ObservationEntity entities;
entities.expression = "<-containedInPlace{typeOf:County, dcid:geoId/06}";

datacommons::ObservationDate date = "LATEST";

auto result = dc.GetObservations(variables, entities, date);
// ... (process results as above)
```

### 3. Resolve

Resolves human-readable identifiers (like names or coordinates) to Data Commons IDs (DCIDs).

**Use Case:** Find the unique DCID for a place when you only know its name.

**Example:** Find the DCIDs for "California" and "Colorado".

```cpp
std::vector<std::string> nodes = {"California", "Colorado"};
std::string property = "<-description->dcid";
auto result = dc.Resolve(nodes, property);

for (const auto& [node, candidates] : result) {
    std::cout << "Node: " << node << std::endl;
    for (const auto& candidate : candidates) {
        std::cout << "  DCID: " << candidate.dcid << ", Type: " << candidate.dominant_type << std::endl;
    }
}
```

### 2. GetObservations

Fetches statistical observations. This endpoint provides a flexible way to query for data by specifying variables, entities, and dates in various combinations.

**Use Case:** Get the total, male, and female population counts for California and Colorado in the year 2020.

**Example:**

```cpp
datacommons::ObservationVariable variables;
variables.dcids = {"Count_Person", "Count_Person_Male", "Count_Person_Female"};

datacommons::ObservationEntity entities;
entities.dcids = {"geoId/06", "geoId/08"};

datacommons::ObservationDate date = "2020";

auto result = dc.GetObservations(variables, entities, date);

for (const auto& [variable, entity_map] : result) {
    std::cout << "Variable: " << variable << std::endl;
    for (const auto& [entity, observations] : entity_map) {
        std::cout << "  Entity: " << entity << std::endl;
        for (const auto& obs : observations) {
            std::cout << "    Date: " << obs.date << ", Value: " << obs.value << std::endl;
        }
    }
}
```

You can also use expressions to select entities, for example, to get the population of all counties in California:

```cpp
datacommons::ObservationVariable variables;
variables.dcids = {"Count_Person"};

datacommons::ObservationEntity entities;
entities.expression = "<-containedInPlace{typeOf:County, dcid:geoId/06}";

datacommons::ObservationDate date = "LATEST";

auto result = dc.GetObservations(variables, entities, date);
// ... (process results as above)
```


### 3. Resolve

Resolves human-readable identifiers (like names or coordinates) to Data Commons IDs (DCIDs).

**Use Case:** Find the unique DCID for a place when you only know its name.

**Example:** Find the DCIDs for "California" and "Colorado".

```cpp
std::vector<std::string> nodes = {"California", "Colorado"};
std::string from_property = "description";
std::string to_property = "dcid";
auto result = dc.Resolve(nodes, from_property, to_property);

for (const auto& [node, candidates] : result) {
    std::cout << "Node: " << node << std::endl;
    for (const auto& candidate : candidates) {
        std::cout << "  DCID: " << candidate.dcid << ", Type: " << candidate.dominant_type << std::endl;
    }
}
```

### 4. Query

Executes a SPARQL query directly against the Data Commons knowledge graph for advanced use cases.

**Use Case:** Retrieve a custom table of data, such as the names and DCIDs of the first 10 states found in the graph.

**Example:**

```cpp
std::string query = "SELECT ?name ?dcid WHERE { ?place typeOf State . ?place name ?name . ?place dcid ?dcid . } LIMIT 10";
auto result = dc.Query(query);

// Print header
for (const auto& header : result.header) {
    std::cout << header << "\t";
}
std::cout << std::endl;

// Print rows
for (const auto& row : result.rows) {
    for (const auto& header : result.header) {
        std::cout << row.at(header) << "\t";
    }
    std::cout << std::endl;
}
```

