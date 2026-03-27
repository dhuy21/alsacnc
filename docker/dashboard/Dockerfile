FROM python:3.11-slim

WORKDIR /opt/app

COPY dashboard/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY dashboard/ dashboard/
COPY database/ database/
COPY shared_utils/ shared_utils/

ENV PYTHONPATH=/opt/app

CMD ["uvicorn", "dashboard.app:app", "--host", "0.0.0.0", "--port", "8000"]
