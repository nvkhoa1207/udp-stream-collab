FROM python:3.13-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY security/ security/
COPY common/ common/
COPY server/ server/
COPY transport/ transport/
COPY client/ client/
COPY ui/ ui/
EXPOSE 9000/tcp 9001/udp
CMD ["python", "-m", "server.main", "--host", "0.0.0.0"]
