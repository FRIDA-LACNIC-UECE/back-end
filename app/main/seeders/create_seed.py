from app.main import db
from app.main.seeders import (
    add_anonymization_records,
    add_database,
    add_database_keys,
    add_sql_logs,
    add_tables,
    add_user,
    create_test_database,
)


def create_seed(env_name: str):
    add_user()
    add_database(env_name=env_name)
    add_database_keys()
    add_sql_logs()
    add_tables()
    db.session.flush()

    if env_name == "dev":
        add_anonymization_records()

        try:
            create_test_database(
                USER="root",
                DB_PW="larces132",
                HOST="localhost",
                DB_NAME="syspad_test_database",
            )
        except:
            print("==== Log Flask Command: create_db ====")
            print("Teste database not created")
    else:
        try:
            create_test_database(
                USER="root",
                DB_PW="",
                HOST="db",
                DB_NAME="syspad_test_database",
            )
        except:
            print("==== Log Flask Command: create_db ====")
            print("Teste database not created")

    db.session.commit()
