from sqlalchemy import func

from app.main import db


class Table(db.Model):
    __tablename__ = "table"
    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)
    database_id = db.Column(db.Integer, db.ForeignKey("database.id"), nullable=False)
    name = db.Column(db.String(100), nullable=False)
    encryption_process = db.Column(db.Integer, nullable=False, default=0)
    anonimyzation_process = db.Column(db.Integer, nullable=False, default=0)
    create_at = db.Column(db.DateTime, server_default=func.now())
    update_at = db.Column(db.DateTime, onupdate=func.now())

    database = db.relationship("Database", back_populates="tables", lazy=True)
    columns = db.relationship("Column", back_populates="table", lazy=True)

    @property
    def encrypted(self):
        return True if self.encryption_process == 100 else False

    @property
    def anonymized(self):
        return True if self.anonimyzation_process == 100 else False

    def __repr__(self):
        return f"<DatabaseId: {self.database_id}, Name: {self.name}, CreateAt: {self.create_at}, UpdateAt: {self.update_at}>"
