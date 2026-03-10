FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src /app/src
COPY scripts /app/scripts
COPY examples /app/examples
COPY README.md /app/README.md
COPY ROADMAP.md /app/ROADMAP.md
COPY TRACE_SCHEMA.md /app/TRACE_SCHEMA.md

ENV PYTHONPATH=/app/src

ENTRYPOINT ["python", "scripts/caps_guard.py"]
CMD ["--help"]
