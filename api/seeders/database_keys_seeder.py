from api import db

from ..model import DatabaseKey
from ..service.rsa_service import generateKeys


def _add_database_keys():
    publicKeyStr, privateKeyStr = generateKeys()
    database_keys = DatabaseKey(1, publicKeyStr, privateKeyStr)

    db.session.add(database_keys)
    db.session.flush()
