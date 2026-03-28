FROM python:3.9-slim

WORKDIR /opt/app

COPY dashboard/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY dashboard/ dashboard/
COPY database/ database/
COPY shared_utils/ shared_utils/
COPY cookie_crawler/crawl_summary.py cookie_crawler/
RUN touch cookie_crawler/__init__.py

ENV PYTHONPATH=/opt/app

EXPOSE 8000
CMD ["sh", "-c", "uvicorn dashboard.app:app --host 0.0.0.0 --port ${PORT:-8000}"]
