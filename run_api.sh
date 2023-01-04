#!/bin/bash
export FLASK_DEBUG="1";
flask create_db;
python3.10 app.py;