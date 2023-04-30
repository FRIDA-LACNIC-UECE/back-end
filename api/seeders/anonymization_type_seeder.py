from api import db

from ..model import AnonymizationType


def _add_anonymization_type():
    cpf_anonymizer = AnonymizationType(name="cpf_anonymizer")
    db.session.add(cpf_anonymizer)
    db.session.flush()

    date_anonymizer = AnonymizationType(name="date_anonymizer")
    db.session.add(date_anonymizer)
    db.session.flush()

    email_anonymizer = AnonymizationType(name="email_anonymizer")
    db.session.add(email_anonymizer)
    db.session.flush()

    ip_anonymizer = AnonymizationType(name="ip_anonymizer")
    db.session.add(ip_anonymizer)
    db.session.flush()

    named_entities_anonymizer = AnonymizationType(name="named_entities_anonymizer")
    db.session.add(named_entities_anonymizer)
    db.session.flush()

    rg_anonymizer = AnonymizationType(name="rg_anonymizer")
    db.session.add(rg_anonymizer)
    db.session.flush()
