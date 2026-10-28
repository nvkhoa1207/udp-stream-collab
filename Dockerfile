FROM python:3.13-slim
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY common/ common/
COPY server/ server/
COPY transport/ transport/
COPY client/ client/
COPY ui/ ui/
COPY scripts/ scripts/
COPY tests/ tests/
EXPOSE 9000/tcp 9001/udp
CMD ["python", "-m", "server.main", "--host", "0.0.0.0"]
