#!/bin/bash
set -e

# Run database migrations for standalone mode
if [ "$1" = "standalone" ]; then
    airflow db migrate

    # Create admin user if it doesn't exist
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin 2>/dev/null || true
fi

# Execute the main command
exec airflow "$@"
