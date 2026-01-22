FROM python:3.13-slim

WORKDIR /app

COPY install.txt .

RUN pip install --no-cache-dir -r install.txt

COPY . .

CMD ["python", "main.py"]