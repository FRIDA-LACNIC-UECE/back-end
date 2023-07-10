#!/bin/bash
export FLASK_APP=application.py

# Uncomment the line below and comment the next line to run in development and debug mode
export ENV_NAME="dev"
#export ENV_NAME="prod"

export DATABASE_URL="mysql://root:trustno1@localhost/frida"

export MAIL_USERNAME="smart.hospital@uece.br"
export MAIL_PASSWORD="uwhlwnievltsujkx"

export SECRET_KEY="w7yHJH5V4l%-oQR/ur\l,q&T?S(4M7eyHLbx/vk\.s?g7?oyAh3+qdtq6GBzmxaz"