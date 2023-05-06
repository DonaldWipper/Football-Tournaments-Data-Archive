FROM prefecthq/prefect:2.8.7-python3.11


COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

