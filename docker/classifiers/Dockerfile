FROM python:3.9


WORKDIR /opt
RUN mkdir repo

WORKDIR /opt/repo
COPY requirements requirements
RUN pip install -r requirements/shared_requirements.txt
RUN pip install torch==1.13.1+cpu --extra-index-url https://download.pytorch.org/whl/cpu
RUN pip install --default-timeout=300 -r requirements/ml_requirements.txt

ENV PYTHONPATH /opt/repo

# Downlaod models
RUN apt-get install unzip
RUN wget https://zenodo.org/records/7638513/files/models.zip &&\
    unzip models.zip && rm models.zip && rm -r __MACOSX

# Pre-cache bert-base-uncased tokenizer (used by predict_purposes and flask_server)
# so it's available offline at runtime without depending on HuggingFace Hub
RUN python -c "from transformers import AutoTokenizer; AutoTokenizer.from_pretrained('bert-base-uncased')"

COPY . .
CMD ["python", "classifiers/text_classifiers/flask_server.py", "--config_path", "config/flask_server_config.yaml"]


