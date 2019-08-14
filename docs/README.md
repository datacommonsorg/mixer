# README

1. Generate Open API JSON file by following [grpc-gateway README](https://github.com/grpc-ecosystem/grpc-gateway/blob/master/README.md) ("Generate swagger definitions") to use `protoc-gen-swagger`.

1. Convert Open API JSON file to YAML file by uploading the JSON file to [Swagger Editor](https://editor.swagger.io/) and downloading as a YAML file.

1. Follow the [Read the Docs Sphinx guide](https://docs.readthedocs.io/en/stable/intro/getting-started-with-sphinx.html) and the [Sphinx Open API extension guide](https://pypi.org/project/sphinxcontrib-openapi/) to configure/build the HTML page that can be hosted on [Read the Docs](https://readthedocs.org).
