FROM python:3.10.6

WORKDIR /api
COPY ./api .

RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 80

ENTRYPOINT ["python"]
CMD ["app.py"]