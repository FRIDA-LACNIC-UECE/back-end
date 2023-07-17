from sqlalchemy import func

from app.main import db


class DatabaseKey(db.Model):
    __tablename__ = "database_key"

    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)
    database_id = db.Column(db.Integer, db.ForeignKey("database.id"), nullable=False)

    public_key = db.Column(db.Text, nullable=False)
    private_key = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, server_default=func.now())
    updated_at = db.Column(db.DateTime, onupdate=func.now())

    database = db.relationship("Database", back_populates="database_key")

    def __repr__(self):
        return f"<Database Key: {self.database_id}"
