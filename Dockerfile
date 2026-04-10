FROM python:3.11-slim

WORKDIR /app

# Copy shared library first so it can be installed in editable mode
COPY ../wnpip-shared-libraray /wnpip-shared-libraray

# Copy orchestrator source
COPY . /app

# Install dependencies (includes -e ../wnpip-shared-libraray which resolves to /wnpip-shared-libraray)
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]
