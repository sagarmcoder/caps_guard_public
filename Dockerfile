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
COPY MVP_GUARD_ROADMAP.md /app/MVP_GUARD_ROADMAP.md
COPY mvpguardpolish.md /app/mvpguardpolish.md

ENV PYTHONPATH=/app/src

ENTRYPOINT ["python", "scripts/caps_guard.py"]
CMD ["--help"]
