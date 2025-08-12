FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
RUN adduser --disabled-password --gecos "" app && chown -R app:app /app
USER app
COPY --chown=app:app requirements.txt .
RUN pip install -r requirements.txt
COPY --chown=app:app core /app/core
COPY --chown=app:app services /app/services
# default command overridden by docker-compose
CMD ["python","-m","services.reddit1.main"]

