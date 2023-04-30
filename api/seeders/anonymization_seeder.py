from api import db

from ..model import Anonymization


def _add_anonymization():
    cpf_anonymization = Anonymization(
        id_database=1, id_anonymization_type=1, table="clientes", columns=["cpf"]
    )
    db.session.add(cpf_anonymization)
    db.session.flush()

    date_anonymization = Anonymization(
        id_database=1,
        id_anonymization_type=2,
        table="clientes",
        columns=["data_de_nascimento"],
    )
    db.session.add(date_anonymization)
    db.session.flush()

    email_anonymization = Anonymization(
        id_database=1,
        id_anonymization_type=3,
        table="clientes",
        columns=["email"],
    )
    db.session.add(email_anonymization)
    db.session.flush()

    ip_anonymization = Anonymization(
        id_database=1,
        id_anonymization_type=4,
        table="clientes",
        columns=["ipv4", "ipv6"],
    )
    db.session.add(ip_anonymization)
    db.session.flush()

    named_entities_anonymization = Anonymization(
        id_database=1,
        id_anonymization_type=5,
        table="clientes",
        columns=["nome"],
    )
    db.session.add(named_entities_anonymization)
    db.session.flush()

    rg_anonymization = Anonymization(
        id_database=1,
        id_anonymization_type=6,
        table="clientes",
        columns=["rg"],
    )
    db.session.add(rg_anonymization)
    db.session.flush()
