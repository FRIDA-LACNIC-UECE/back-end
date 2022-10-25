import os
from flask import redirect
from flask_migrate import Migrate

from model import (
    user_model, valid_database_model, 
    database_model, database_key_model, 
    anonymization_type_model, anonymization_model, 
)

from controller import (
    app, db, database_controller, user_controller, 
    valid_database_controller, rsa_controller, sse_controller,
    anonymization_controller, anonymization_type_controller
)


Migrate(app, db)

@app.shell_context_processor
def make_shell_context():
    return dict(
        app=app,
        db=db,
        User=user_model.User
    )

@app.route('/')
def index():
    return redirect("https://github.com/FRIDA-LACNIC-UECE", code=302)


if __name__ == "__main__":
    os.system("flask db init")
    os.system('flask db migrate -m "Initial migration"')
    os.system('flask db upgrade')

    app.run(host="0.0.0.0", port=5000, debug=True)