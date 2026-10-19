FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 CLOUDPULSE_DB_PATH=/data/cloudpulse.db
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install ".[all]" && useradd --system --create-home cloudpulse && mkdir /data && chown cloudpulse /data
USER cloudpulse
VOLUME /data
EXPOSE 8000 8501
ENTRYPOINT ["cloudpulse"]
CMD ["serve", "--host", "0.0.0.0"]
