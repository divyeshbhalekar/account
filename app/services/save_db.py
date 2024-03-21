from app.schemas.db import ServiceAccountD,ServiceAccountM,ServiceAccountY
import logging
from datetime import datetime, timezone, timedelta
from dateutil.parser import parse
from dateutil import parser,  tz


from dateutil.relativedelta import relativedelta

FORMAT = '%(asctime)s\t| %(levelname)s | line %(lineno)d, %(name)s, func: %(funcName)s()\n%(message)s\n'
logging.basicConfig(format=FORMAT)

logger = logging.getLogger("scave_db")
logger.setLevel(level=logging.INFO)



class ACCOUNTSERVICE:
    def __init__(self,requests):
        self.scan_id = requests.get('scan_id',None).split(';')
        self.company_id = requests.get('company_id',None)
        self.profile_id = requests.get('profile_id',None)
        self.status_code = requests.get('status_code',None)
        self.asset_type = requests.get('asset_type',None)
        self.entity_count = requests.get('entity_count', None)  # Default to None if not present
        self.metadata = requests.get('metadata',None)
        self.failure_message = requests.get('failure_message',None)
        self.scan_start_time = datetime_convertion(requests.get('scan_start_time',None))
        self.scan_completed_time = datetime_convertion(requests.get('scan_completed_time',None))


    def save_db(self,col):
        if col=='daily':
            data_db=ServiceAccountD()
        elif col=='monthly':
            data_db=ServiceAccountM()   
        elif col=='yearly':
            data_db=ServiceAccountY() 

        if 'serverless' in self.metadata:
            no_of_functions_value = self.metadata['serverless']['no_of_functions']
            if no_of_functions_value.find('x') != -1:
                integer=int(no_of_functions_value[2:], 16) 
            else:
                integer = int(no_of_functions_value)
            self.metadata['serverless']['no_of_functions'] = integer
        

        if self.scan_id:
            data_db.scan_id = self.scan_id
        if self.company_id:
            data_db.company_id = self.company_id    
        if self.profile_id:
            data_db.profile_id = self.profile_id
        if self.status_code:
            data_db.status_code = self.status_code
        if self.asset_type:
            data_db.asset_type = self.asset_type
        if self.entity_count:
            data_db.entity_count = self.entity_count
        elif self.entity_count:
            data_db.entity_count = 0    
        if self.metadata:
            data_db.metadata = self.metadata
        elif self.metadata:
            data_db.metadata = {}   
        if self.failure_message:
            data_db.failure_message = self.failure_message
        elif self.failure_message:
            data_db.failure_message = ""    
        if self.scan_start_time:
            data_db.scan_start_time = self.scan_start_time
        if self.scan_completed_time:
            data_db.scan_completed_time = self.scan_completed_time
        data_db.save()
        # logger.info(f"Data saved to the accounting db with profileid :{self.profile_id}")


def datetime_convertion(date):
    dt=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
    if date:
        if not str(date).isdigit(): 
            if 'UTC' in date:
                parsed_datetime = parser.parse(date.split('UTC')[0], ignoretz= True)
            else:
                parsed_datetime = parser.parse(date, ignoretz= True)
            try:
                dt=parsed_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
            except Exception as e:
                utc_datetime = parsed_datetime.astimezone(tz.UTC)
                dt=utc_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
                return str(dt)
        else:
            utc_datetime = datetime.utcfromtimestamp(date)          
            dt=utc_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
    return str(dt)
    # if date:
    #     if type(date)!=int: 
    #         print(1)
    #         parsed_datetime = parser.parse(date, ignoretz= True)
    #         print(2)
    #         dt=parsed_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
    #     elif type(date)==int:
    #         utc_datetime = datetime.utcfromtimestamp(date)
    #         utc_datetime1 = utc_datetime.replace(tzinfo=timezone.utc)          
    #         dt=utc_datetime1.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
    # return str(dt)