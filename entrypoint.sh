#!/bin/bash
set -e
flask create_db;
python application.py;
