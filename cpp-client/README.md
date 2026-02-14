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

## Building and Running the Example

### Prerequisites

- C++17 compiler (g++ or Clang)
- CMake (3.11+)
- Git
- OpenSSL development libraries (`libssl-dev` on Debian/Ubuntu)

### Steps

1.  **Clone the repository and navigate to the client directory.**
2.  **Create a build directory:**
    ```bash
    mkdir build
    cd build
    ```
3.  **Configure and build the project:**
    ```bash
    cmake ..
    make
    ```
4.  **Set your API key:**
    ```bash
    export DC_API_KEY="YOUR_API_KEY"
    ```
5.  **Run the example:**
    ```bash
    ./example
    ```

## Usage

See the `USAGE.md` file for a detailed guide to the library's functions.
