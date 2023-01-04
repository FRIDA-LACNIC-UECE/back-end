from api import db
from api.seeders import (
    _add_anonymization,
    _add_anonymization_type,
    _add_database,
    _add_database_keys,
    _add_user,
    _add_valid_database,
    _create_test_frida_db,
)


def _create_seeder():
    # To seed tables
    _add_user()
    _add_valid_database()
    _add_database()
    _add_database_keys()
    _add_anonymization_type()
    _add_anonymization()
    db.session.commit()

    # Create Test Database
    try:
        _create_test_frida_db(
            USER="root",
            DB_PW="",
            HOST="db",
            DB_NAME="test_frida_db",
        )
    except:
        print("==== Log Create DB Flask ====")
        print("Teste database not created")
