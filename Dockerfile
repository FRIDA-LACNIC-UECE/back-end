FROM python:3.10.6


WORKDIR /app

RUN python -m pip install --upgrade pip

COPY . .

RUN pip install -r ./requirements.txt

EXPOSE 5000
ENV ENV_NAME=staging
ENV FLASK_APP=application
COPY entrypoint.sh .


# RUN ["pytest"]

RUN ["chmod", "+x", "./entrypoint.sh"]
ENTRYPOINT ["./entrypoint.sh"]
