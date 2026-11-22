FROM redis:7.2.5-bookworm@sha256:3aaec283e6e593bde528077d60280ac1589887067a39273348860837c9346d7e AS redis

FROM python:3.12.4-slim-bookworm@sha256:a3e58f9399353be051735f09be0316bfdeab571a5c6a24fd78b92df85bcb2d85

# redis-server is only used by the integration tests.
COPY --from=redis /usr/local/bin/redis-server /usr/local/bin/

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY payapi/ payapi/
COPY tests/ tests/

ENV HOST=0.0.0.0 PORT=8080
EXPOSE 8080
CMD ["python", "-m", "payapi"]
