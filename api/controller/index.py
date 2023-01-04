from flask import redirect

from api import app


@app.route("/", methods=["GET"])
def index():
    return redirect("https://github.com/FRIDA-LACNIC-UECE", code=302)
