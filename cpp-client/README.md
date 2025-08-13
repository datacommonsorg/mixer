# Data Commons C++ Client Library

A C++ client library for accessing the Data Commons API.

## Authentication

The recommended way to provide your API key is by setting the `DC_API_KEY` environment variable. The client will automatically detect and use it.

```bash
export DC_API_KEY="YOUR_API_KEY"
```

Alternatively, you can pass the key directly to the constructor:

```cpp
#include "DataCommons.h"

int main() {
    datacommons::DataCommons dc("YOUR_API_KEY");
    // ...
    return 0;
}
```

## Usage

See the `examples/main.cpp` file for a demonstration of how to use the library.
