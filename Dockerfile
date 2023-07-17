FROM python:3.10.6


WORKDIR /app

RUN python -m pip install --upgrade pip

COPY . .

RUN pip install -r ./requirements.txt

EXPOSE 5000

ENV MAIL_USERNAME=$MAIL_USERNAME
ENV MAIL_PASSWORD=$MAIL_PASSWORD
ENV SECRET_KEY=$SECRET_KEY
ENV ENV_NAME=staging
ENV FLASK_APP=application

COPY entrypoint.sh .
RUN ["chmod", "+x", "./entrypoint.sh"]
ENTRYPOINT ["./entrypoint.sh"]
