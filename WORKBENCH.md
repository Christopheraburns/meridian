# Deploy as Cloudera AI Workbench Application

1. Create a Workbench project with a compatible Python 3.11 runtime and load the extracted source code into its root.
2. Install packages with `pip install -r requirements.txt` using the project's configured package repository.
3. Configure the application launch script as `app.py`. It listens on `CDSW_APP_PORT` automatically.
4. Set `GENERATOR_OUTPUT` to a durable project directory with sufficient quota. Run a 100K-row test first.
5. Use only permitted Workbench project users. The UI has no application-level login.
6. For production-sized jobs use a Workbench Job or scalable data-engine execution rather than depending on the lifetime of the web application process.
7. Transfer generated Parquet to governed storage and create/populate Iceberg tables using approved Spark/Impala integration for your cluster and catalog. No automatic Iceberg commits are performed in v0.1.
8. Keep `_private_ground_truth` inaccessible to the investigation MCP server, Mistral agents, and ordinary query roles.

**Image difference:** Docker Desktop uses the included `python:3.11-slim` image. A Cloudera custom runtime must use a compatible official Cloudera base image, chosen for your Workbench version, not this generic Dockerfile.
