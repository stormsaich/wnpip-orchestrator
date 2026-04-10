FROM python:3.11-slim

WORKDIR /app

# Install shared library from local source (checked out as sibling by CI).
COPY wnpip-shared-libraray/ wnpip-shared-libraray/
RUN pip install --no-cache-dir ./wnpip-shared-libraray

# Install remaining dependencies.
COPY wnpip-orchestrator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy service source.
COPY wnpip-orchestrator/ .

CMD ["python", "main.py"]
