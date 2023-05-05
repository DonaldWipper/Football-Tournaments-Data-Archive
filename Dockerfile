FROM prefecthq/prefect:2-python3.9-kubernetes


COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

