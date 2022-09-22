FROM python:3.10.6

WORKDIR /agent
COPY /agent /agent

RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5000

ENTRYPOINT ["python"]
CMD ["main.py"]