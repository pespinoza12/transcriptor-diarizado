FROM python:3.11-slim

RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p whatsapp_audios processados

EXPOSE 5050

CMD ["gunicorn", "app:app", \
     "--bind", "0.0.0.0:5050", \
     "--workers", "1", \
     "--threads", "4", \
     "--timeout", "600"]
