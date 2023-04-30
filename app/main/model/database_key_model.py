from app.main import db


class DatabaseKey(db.Model):
    __tablename__ = "database_key"

    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)
    database_id = db.Column(db.Integer, db.ForeignKey("database.id"), nullable=False)
    public_key = db.Column(db.Text, nullable=False)
    private_key = db.Column(db.Text, nullable=False)

    database = db.relationship("Database", back_populates="database_key", lazy=True)

    def __repr__(self):
        return f"<DatabaseKey : {self.database_id}, public_key: {self.public_key, }private_key : {self.private_key}>"
