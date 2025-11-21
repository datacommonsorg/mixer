FROM python:3.10-slim-bullseye

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY locustfiles .

CMD ["locust", "-f", "v2_observation.py"]
