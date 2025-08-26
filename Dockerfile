FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
RUN adduser --disabled-password --gecos "" app && chown -R app:app /app
USER app
COPY --chown=app:app requirements.txt .
RUN pip install -r requirements.txt
COPY --chown=app:app core /app/core
COPY --chown=app:app services /app/services
COPY --chown=app:app config /app/config
COPY --chown=app:app ops/healthcheck.py /app/ops/healthcheck.py

HEALTHCHECK --interval=5m --timeout=10s --start-period=1m --retries=3 \
  CMD ["python", "/app/ops/healthcheck.py"]

# default command overridden by docker-compose. 
# To run the new worker, you would use:
# CMD ["python", "-m", "services.signals_worker.main"]
CMD ["python", "-m", "services.reddit1.main"]

