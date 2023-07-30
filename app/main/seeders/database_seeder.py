from app.main import db
from app.main.model import Database


def add_database(env_name: str):
    if env_name == "dev":
        database_test = Database(
            user_id=2,
            valid_database_id=1,
            name="syspad_test_database",
            username="root",
            host="localhost",
            port=3306,
            password="larces132",
        )
    else:
        database_test = Database(
            user_id=2,
            valid_database_id=1,
            name="syspad_test_database",
            username="root",
            host="db",
            port=3306,
            password="",
        )
    db.session.add(database_test)

    new_database = Database(
        user_id=2,
        valid_database_id=1,
        name="syspad_test_database_2",
        username="root",
        host="localhost",
        port=3306,
        password="senha123",
    )
    db.session.add(new_database)

    new_database = Database(
        user_id=2,
        valid_database_id=1,
        name="syspad_test_database_3",
        username="root",
        host="localhost",
        port=3306,
        password="senha123",
    )
    db.session.add(new_database)

    new_database = Database(
        user_id=3,
        valid_database_id=2,
        name="syspad_test_database_4",
        username="postgres",
        host="localhost",
        port=3306,
        password="senha123",
    )
    db.session.add(new_database)

    new_database = Database(
        user_id=3,
        valid_database_id=2,
        name="syspad_test_database_5",
        username="postgres",
        host="localhost",
        port=3306,
        password="senha123",
    )
    db.session.add(new_database)

    db.session.flush()
