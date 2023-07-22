from sqlalchemy import func

from app.main import db


class Table(db.Model):
    __tablename__ = "table"
    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)
    database_id = db.Column(db.Integer, db.ForeignKey("database.id"), nullable=False)

    name = db.Column(db.String(100), nullable=False)
    encryption_progress = db.Column(db.Integer, nullable=False, default=0)
    anonymization_progress = db.Column(db.Integer, nullable=False, default=0)
    create_at = db.Column(db.DateTime, server_default=func.now())
    update_at = db.Column(db.DateTime, onupdate=func.now())

    database = db.relationship("Database", back_populates="tables", lazy=True)
    anonymization_records = db.relationship(
        "AnonymizationRecord", back_populates="table"
    )

    @property
    def encrypted(self):
        return True if self.encryption_progress >= 100 else False

    @property
    def anonymized(self):
        return True if self.anonymization_progress >= 100 else False

    @property
    def anonymization_status(self):
        if self.anonymization_progress <= 0:
            return "not_anonymized"
        elif self.anonymization_progress >= 100:
            return "anonymized"
        else:
            return "anonymization_in_progress"

    @property
    def encryption_status(self):
        if self.encryption_progress <= 0:
            return "not_encrypted"
        elif self.encryption_progress >= 100:
            return "encrypted"
        else:
            return "encryption_in_progress"

    def __repr__(self):
        return f"<Table: {self.id}>"
