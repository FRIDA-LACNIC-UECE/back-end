from app.main.model import DatabaseKey


def get_database_keys_by_database_id(database_id: int) -> None:
    return DatabaseKey.query.filter(DatabaseKey.database_id == database_id).first()
