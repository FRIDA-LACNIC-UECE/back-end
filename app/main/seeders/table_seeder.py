from app.main import db
from app.main.model import Database, Table


def add_tables():
    database = Database.query.get(1)

    new_table = Table(name="clientes", database=database)
    db.session.add(new_table)

    new_table = Table(name="fornecedores", database=database)
    db.session.add(new_table)

    db.session.flush()
