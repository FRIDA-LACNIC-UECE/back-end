FROM python:3.10.6

WORKDIR /app

RUN python -m pip install --upgrade pip

COPY . .

RUN pip install -r ./requirements.txt

EXPOSE 5000

RUN ["chmod", "+x", "./entrypoint.sh"]
ENTRYPOINT ["./entrypoint.sh"]