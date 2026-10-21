# Cloudera AI Workbench

1. Load this repository into a project using an administrator-approved Python 3.11
   runtime. Install requirements.txt from the approved package repository.
2. For an Application use app.py; CDSW_APP_PORT remains supported. Restrict access
   using platform controls. The legacy UI has no application-level login and its
   volatile daemon worker is pending replacement in M3.
3. For M1 use a Workbench Job or terminal to run:
   `python3 cli.py --config config/smoke.yaml --output /durable/public/run-001 --private-output /restricted/private/run-001`.
   Substitute actual approved writable storage paths; directories must be empty.
4. Public output and private truth need separate access controls and persistent
   storage. Do not expose the private directory to MCP, agents or ordinary query
   roles. Do not assume the project root is persistent or writable.
5. Use a compatible official Cloudera base image for custom runtimes. The included
   generic Dockerfile remains intended for Docker Desktop only.

No table creation, schema alteration or Iceberg commits occur in M1. Preserve the
already-created tables. M4 requires approved Spark/catalog configuration and
separate credentials before any integration write. See
[the M4 dependency checklist](docs/runtime_assumptions.md#information-required-for-m4).
No authorization checks or live Impala/Spark/Iceberg connectivity have been tested.
