#!/bin/bash
set -e

# Run database migrations for standalone mode
if [ "$1" = "standalone" ]; then
    airflow db migrate

    # Fixed admin/admin login: the simple auth manager only generates passwords
    # for users missing from this file
    echo '{"admin": "admin"}' > "${AIRFLOW_HOME}/simple_auth_manager_passwords.json.generated"
fi

# Execute the main command
exec airflow "$@"
