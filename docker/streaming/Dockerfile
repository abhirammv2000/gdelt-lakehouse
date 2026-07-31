# Long-running GDELT stream consumer. The package is mounted at /opt/gdelt/src
# (PYTHONPATH), so code edits are live; only its runtime deps are baked in.
FROM python:3.11-slim

COPY docker/streaming/requirements.txt /tmp/req.txt
RUN pip install --no-cache-dir -r /tmp/req.txt

ENV PYTHONPATH=/opt/gdelt/src
WORKDIR /opt/gdelt

CMD ["python", "-m", "gdelt_pipeline.streaming"]
