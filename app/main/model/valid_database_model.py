from app.main import db


class ValidDatabase(db.Model):
    __tablename__ = "valid_database"

    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)
    name = db.Column(db.String(100), nullable=False, unique=True)

    database = db.relationship("Database", back_populates="valid_database", lazy=True)

    def __repr__(self):
        return f"<ValidDatabase {self.name}>"
