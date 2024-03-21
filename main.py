import os
from dotenv import load_dotenv
import uvicorn
import logging
import threading

load_dotenv()

FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
logging.basicConfig(format=FORMAT)


from app import app, config, main_func

if __name__=="__main__":
    thread=threading.Thread(target=main_func)
    thread.start()
    uvicorn.run("main:app", host=config.HOST, port=config.PORT)