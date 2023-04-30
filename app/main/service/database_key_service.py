from app.main.model import DatabaseKey
import base64

import rsa


def get_database_keys_by_database_id(database_id: int) -> None:
    return DatabaseKey.query.filter(DatabaseKey.database_id == database_id).first()


def generate_keys():
    # Generating keys
    (publicKey, privateKey) = rsa.newkeys(2048)

    # Save in PEM format
    publicKeyPEM = publicKey.save_pkcs1("PEM")
    privateKeyPEM = privateKey.save_pkcs1("PEM")

    # Transform from PEM to string base64
    publicKeyStr = str(base64.b64encode(publicKeyPEM))[2:-1]
    privateKeyStr = str(base64.b64encode(privateKeyPEM))[2:-1]

    return publicKeyStr, privateKeyStr
