from app.main import db
from app.main.model import AnonymizationRecord


def add_anonymization_records():
    cpf_anonymization = AnonymizationRecord(
        database_id=1, anonymization_type_id=1, table="clientes", columns=["cpf"]
    )
    db.session.add(cpf_anonymization)
    db.session.flush()

    date_anonymization = AnonymizationRecord(
        database_id=1,
        anonymization_type_id=2,
        table="clientes",
        columns=["data_de_nascimento"],
    )
    db.session.add(date_anonymization)
    db.session.flush()

    email_anonymization = AnonymizationRecord(
        database_id=1,
        anonymization_type_id=3,
        table="clientes",
        columns=["email"],
    )
    db.session.add(email_anonymization)
    db.session.flush()

    ip_anonymization = AnonymizationRecord(
        database_id=1,
        anonymization_type_id=4,
        table="clientes",
        columns=["ipv4", "ipv6"],
    )
    db.session.add(ip_anonymization)
    db.session.flush()

    named_entities_anonymization = AnonymizationRecord(
        database_id=1,
        anonymization_type_id=5,
        table="clientes",
        columns=["nome"],
    )
    db.session.add(named_entities_anonymization)
    db.session.flush()

    rg_anonymization = AnonymizationRecord(
        database_id=1,
        anonymization_type_id=6,
        table="clientes",
        columns=["rg"],
    )
    db.session.add(rg_anonymization)
    db.session.flush()
