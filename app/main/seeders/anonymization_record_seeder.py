from app.main import db
from app.main.model import AnonymizationRecord


def add_anonymization_records():
    cpf_anonymization = AnonymizationRecord(
        table_id=1, anonymization_type_id=1, columns=["cpf"]
    )
    db.session.add(cpf_anonymization)

    date_anonymization = AnonymizationRecord(
        table_id=1,
        anonymization_type_id=2,
        columns=["data_de_nascimento"],
    )
    db.session.add(date_anonymization)

    email_anonymization = AnonymizationRecord(
        table_id=1,
        anonymization_type_id=3,
        columns=["email"],
    )
    db.session.add(email_anonymization)

    ip_anonymization = AnonymizationRecord(
        table_id=1,
        anonymization_type_id=4,
        columns=["ipv4", "ipv6"],
    )
    db.session.add(ip_anonymization)

    named_entities_anonymization = AnonymizationRecord(
        table_id=1,
        anonymization_type_id=5,
        columns=["nome"],
    )
    db.session.add(named_entities_anonymization)

    rg_anonymization = AnonymizationRecord(
        table_id=1,
        anonymization_type_id=6,
        columns=["rg"],
    )
    db.session.add(rg_anonymization)

    db.session.flush()
