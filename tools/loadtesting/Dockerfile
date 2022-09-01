FROM python:3.10-slim-bullseye

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY locustfiles .

CMD ["locust", "-f", "bulk_observations_series_linked.py"]
