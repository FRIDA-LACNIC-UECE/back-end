from app.main import db


class AnonymizationRecord(db.Model):
    __tablename__ = "anonymization_record"

    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)
    database_id = db.Column(db.Integer, db.ForeignKey("database.id"), nullable=False)
    anonymization_type_id = db.Column(
        db.Integer, db.ForeignKey("anonymization_type.id"), nullable=False
    )
    table = db.Column(db.String(100), nullable=False)
    columns = db.Column(db.JSON, nullable=False)

    database = db.relationship(
        "Database", back_populates="anonymization_record", lazy=True
    )
    anonymization_type = db.relationship(
        "AnonymizationType", back_populates="anonymization_record", lazy=True
    )

    def __repr__(self):
        return f"<AnonymizationRecord {self.anonymization_type_id} - {self.table} - {self.columns}>"
