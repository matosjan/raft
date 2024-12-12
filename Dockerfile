FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install flask requests

COPY node.py .

CMD ["python", "-u", "node.py"]
