from app.main import db


class AnonymizationType(db.Model):
    __tablename__ = "anonymization_type"

    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)
    name = db.Column(db.String(100), nullable=False, unique=True)

    anonymization_record = db.relationship(
        "AnonymizationRecord", back_populates="anonymization_type", lazy=True
    )

    def __repr__(self):
        return f"<AnonymizationType : {self.name}>"
