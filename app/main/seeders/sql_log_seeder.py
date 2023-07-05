from app.main import db
from app.main.model import Database, SqlLog


def add_sql_logs():
    database = Database.query.all()

    new_sql_log = SqlLog(
        database=database[0],
        sql_command="SELECT * FROM teste1",
    )
    db.session.add(new_sql_log)

    new_sql_log = SqlLog(
        database=database[0],
        sql_command="SELECT * FROM teste1",
    )
    db.session.add(new_sql_log)

    new_sql_log = SqlLog(
        database=database[1],
        sql_command="SELECT * FROM teste2",
    )
    db.session.add(new_sql_log)

    new_sql_log = SqlLog(
        database=database[1],
        sql_command="SELECT * FROM teste2",
    )
    db.session.add(new_sql_log)

    new_sql_log = SqlLog(
        database=database[2],
        sql_command="DELETE FROM teste3",
    )
    db.session.add(new_sql_log)

    new_sql_log = SqlLog(
        database=database[3],
        sql_command="SELECT * FROM teste4",
    )
    db.session.add(new_sql_log)

    new_sql_log = SqlLog(
        database=database[3],
        sql_command="DELETE FROM teste4",
    )
    db.session.add(new_sql_log)

    new_sql_log = SqlLog(
        database=database[3],
        sql_command="SELECT * FROM teste5",
    )
    db.session.add(new_sql_log)

    new_sql_log = SqlLog(
        database=database[3],
        sql_command="DELETE FROM teste5",
    )
    db.session.add(new_sql_log)

    db.session.flush()
