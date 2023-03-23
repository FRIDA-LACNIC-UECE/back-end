#!/bin/bash
set -e
flask create_db;
python3.10 application.py;