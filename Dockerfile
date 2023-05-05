FROM prefecthq/prefect:2.10.7-python3.9


COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

