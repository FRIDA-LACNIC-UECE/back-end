FROM python:3.11.1
WORKDIR flask-app
COPY app ./app
COPY application.py .
COPY seeder.py .
COPY requirements.txt .

RUN pip install -r requirements.txt

EXPOSE 5000
ENV ENV_NAME=staging
ENV FLASK_APP=application
COPY entrypoint.sh .

# RUN ["pytest"]

RUN ["chmod", "+x", "./entrypoint.sh"]
ENTRYPOINT ["./entrypoint.sh"]
