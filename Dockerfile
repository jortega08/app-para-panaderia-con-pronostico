FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install Python dependencies first to keep the build cache effective.
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Copy only the application sources into the final image.
COPY . .

RUN mkdir -p /app/backups /app/data \
    && groupadd --system app \
    && useradd --system --gid app --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin app \
    && chown -R app:app /app

USER app

EXPOSE 5000

CMD ["gunicorn", "-c", "gunicorn.conf.py", "wsgi:app"]
