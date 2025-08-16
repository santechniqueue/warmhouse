FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install --no-cache-dir fastapi uvicorn[standard] asyncpg
COPY main.py .
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8083"]
