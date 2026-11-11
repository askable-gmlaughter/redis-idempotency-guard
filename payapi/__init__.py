from flask import Flask

from . import db
from .payments import bp


def create_app():
    db.init()
    app = Flask(__name__)
    app.register_blueprint(bp)
    return app
