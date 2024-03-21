from core.config import config
from fastapi import FastAPI, BackgroundTasks
# from blacksheep import Application, Request
# from blacksheep.server.openapi.v3 import OpenAPIHandler
# from openapidocs.v3 import Info
from mongoengine import connect
# from mongoengine.connection import ConnectionError
from app.rabbitmq.consumer import Consumer
import logging
import os
from app.services.fetch_data import fetch
from app.schemas.db import AccountInCreate,FetchScanSettings
from typing import List
from app.services.scan_option import send_scan_data


logger = logging.getLogger("app_info_initialize")
logger.setLevel(level=logging.INFO)

app =FastAPI()

# app = Application()
# docs = OpenAPIHandler(info=Info(title="Account Service API", version="0.0.1"))
# docs.bind_app(app)

 
def rabbitmq_connection():
    username = os.getenv('RABBITMQ_USERNAME','guest')
    password = os.getenv('RABBITMQ_PASSWORD','guest')
    host = os.getenv('RABBITMQ_HOST','localhost')
    port = os.getenv('RABBITMQ_PORT','5672')
    amqp_url = f'amqp://{username}:{password}@{host}:{port}/%2F'
    consumer = Consumer(amqp_url)
    consumer.run()

# def connect_to_mongo():
#     try:
#         global db
#         db = connect(
#             'account-service',
#             host='localhost',
#             port=27017,
#             username=None,
#             password=None,
#             authentication_source="admin",
#         )
#         logger.info("Mongodb connection successful")
#     except Exception as e:
#         print(e)
#         logger.error("unable to connect to the database")

# connect_to_mongo()

def create_db_client():
    logger.info("Bootstrapping started")
    global db
    try:
        logger.info(f"MongoDB URL: {config.MONGODB_URL}")
        db = connect(host=str(config.MONGODB_URL))
        # info = db.server_info()
        logger.info(f"MongoDB is now connected { db}" )
        # Add authentication if needed
    except ConnectionError as ce:
        logger.error("Error connecting to MongoDB: %s", ce)
        raise ce  # Reraise the exception to handle it at a higher level
    except Exception as e:
        logger.exception("Unexpected error during MongoDB connection", e)
        exit(1)

    logger.info("Bootstrapping is now done")


async def shutdown_db_client():
    try:
        db.client.close()
        logger.info("MongoDB connection closed")
    except Exception as e:
        logger.exception("Error during MongoDB connection shutdown", e)


def main_func():
    create_db_client()
    rabbitmq_connection()


@app.get("/health")
def health_check():
    return {"message": "Service is running well"}


@app.post("/fetch_data")
async def fetch_accounting_db(req:AccountInCreate,bg:BackgroundTasks):
    data=fetch(req)
    return data   #{"message": "Request for data fetch Recieved"}


@app.post("/send/scan_settings")
async def send_scan_setting(req:FetchScanSettings,bg:BackgroundTasks):
    status=send_scan_data(req)
    return status
