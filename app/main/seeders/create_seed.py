from app.main import db
from app.main.seeders import (
    add_anonymization_records,
    add_columns,
    add_database,
    add_database_keys,
    add_sql_logs,
    add_tables,
    add_user,
    create_test_frida_db,
)


def create_seed(env_name: str):
    add_user()
    add_database(env_name=env_name)
    add_database_keys()
    add_sql_logs()
    add_tables()
    add_columns()
    db.session.flush()

    if env_name == "dev":
        add_anonymization_records()

        try:
            create_test_frida_db(
                USER="root",
                DB_PW="larces132",
                HOST="localhost",
                DB_NAME="test_frida_db",
            )
        except:
            print("==== Log Flask Command: create_db ====")
            print("Teste database not created")
    else:
        try:
            create_test_frida_db(
                USER="root",
                DB_PW="",
                HOST="db",
                DB_NAME="test_frida_db",
            )
        except:
            print("==== Log Flask Command: create_db ====")
            print("Teste database not created")

    db.session.commit()
