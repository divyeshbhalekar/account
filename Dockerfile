FROM python:3.8-slim

LABEL maintainer="Vikash"

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt


CMD ["python", "main.py"]
# ENTRYPOINT ["sh", "script.sh"]

EXPOSE 8000