from app.services.save_db import ACCOUNTSERVICE
from app.schemas.db import ServiceAccountD,ServiceAccountM,ServiceAccountY
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from app.services.save_db import datetime_convertion



FORMAT = '%(asctime)s\t| %(levelname)s | line %(lineno)d, %(name)s, func: %(funcName)s()\n%(message)s\n'
logging.basicConfig(format=FORMAT)

logger = logging.getLogger("update_db")
logger.setLevel(level=logging.INFO)

class PROCESSMSG:
    def __init__(self,message): 
        self.message=message
        self.company_id=message['company_id']
        self.profile_id=message['profile_id']
        self.asset_type=message['asset_type']
        self.metadata=message['metadata']
        self.scan_id=message['scan_id']
        self.scan_completed_time=message['scan_completed_time']
        self.today = datetime.now(timezone.utc)
        # today = datetime.now(timezone.utc).date()

    async def processing_msg(self):
        ACCOUNTSERVICE(self.message).save_db('daily')
        self.monthly_function()
        self.yearly_function()
        logger.info(f"message has been saved to the db with profileid {self.profile_id} and companyid {self.company_id} and asset type {self.asset_type}")
    
    def monthly_function(self):
        start_time = self.today.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = self.today.replace(hour=23, minute=59, second=59, microsecond=999999)
        # startm = (self.today - relativedelta(months=0)).replace(day=1, hour=0, minute=0, second=0)
        # endm = self.today.replace(hour=23, minute=59, second=59)
        
        startm_str = start_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
        endm_str = end_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
        service=ServiceAccountM.objects.filter(
                        company_id=self.company_id, profile_id=self.profile_id,asset_type=self.asset_type,
                        scan_start_time__gte=startm_str, scan_start_time__lt=endm_str).first()
    
        if service:
            data=service.to_dict()
            if type(self.scan_completed_time)==int:
                today = datetime_convertion(self.scan_completed_time).split('T')[0]
            else:
                today = datetime_convertion(str(self.scan_completed_time)).split('T')[0] 
            date_rec=datetime_convertion(str(data['scan_completed_time'])).split('T')[0]
            if today==date_rec:
                self.count_function(service)
            else:
                ACCOUNTSERVICE(self.message).save_db('monthly')
        elif service==None:
            ACCOUNTSERVICE(self.message).save_db('monthly')
            

    def yearly_function(self):
        start_date = self.today - relativedelta(day=1, hours=self.today.hour, minutes=self.today.minute, seconds=self.today.second)
        end_date = start_date + relativedelta(months=1, days=-1, hours=23, minutes=59, seconds=59)
        # starty = (self.today - relativedelta(months=0)).replace(day=1, hour=0, minute=0, second=0)
        # endy = self.today.replace(day=31, hour=23, minute=59, second=59)
        starty_str = start_date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
        endy_str = end_date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
        service=ServiceAccountY.objects.filter(
                        company_id=self.company_id, profile_id=self.profile_id,asset_type=self.asset_type,
                        scan_start_time__gte=starty_str, scan_start_time__lt=endy_str).first()
        if service:
            data=service.to_dict()
            
            if type(self.scan_completed_time)==int:
                today = datetime_convertion(self.scan_completed_time).split('T')[0].split('-')[1]
            else:
                today = datetime_convertion(str(self.scan_completed_time)).split('T')[0].split('-')[1]  
            date_rec=datetime_convertion(str(data['scan_completed_time'])).split('T')[0].split('-')[1]
            if today==date_rec:
                self.count_function(service)
            else:
                ACCOUNTSERVICE(self.message).save_db('yearly')    
        elif service==None:
            ACCOUNTSERVICE(self.message).save_db('yearly')
            
    def count_function(self,service):
        data=service.to_dict()
        db1=self.metadata[self.asset_type]
        db2=data['metadata'][self.asset_type]
        value=self.dict_count(db1,db2)
        for k,v in value.items():
            if str(v).isdigit():
                field_name = f"metadata__{self.asset_type}__{k}"
                service.modify(
                            upsert=True,
                            **{f'set__{field_name}': int(v)},
                            add_to_set__scan_id= self.scan_id.split(',')
                            )            
            

    def dict_count(self,db1,db2):
        for k,l in db1.items():
            if str(l).isdigit():
                if 'x' in str(l):
                    db2[k]+=int(l[2:], 16)  
                else:  
                    db2[k]+=int(l)  
        return  db2 

    
async def process_message(message):
    await asyncio.sleep(1)
    asyncio.create_task(PROCESSMSG(message).processing_msg())

