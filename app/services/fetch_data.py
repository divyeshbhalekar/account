from app.schemas.db import ServiceAccountM as asdbm
from app.schemas.db import ServiceAccountY as asdby
from app.schemas.db import ServiceAccountD as asdbd
import logging
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta


FORMAT = '%(asctime)s\t| %(levelname)s | line %(lineno)d, %(name)s, func: %(funcName)s()\n%(message)s\n'
logging.basicConfig(format=FORMAT)

logger = logging.getLogger("fetch_data")
logger.setLevel(level=logging.INFO)


def fetch(req):
    today = datetime.now().replace(hour=23, minute=59, second=59)
    first_day_prev_month = today.replace(day=1) - timedelta(days=1)
    prev_month_start = first_day_prev_month.replace(day=1)
    fetch_data=None   
    if not (req.daily or req.monthly or req.yearly):
        #with asset type
        try:
            if req.asset_type or req.asset_type[0] not in ("", "string"):
                fetch_data = True
        except Exception as e:
            pass  

        #without asset type      
        if not req.asset_type:
            fetch_data = False
        elif req.asset_type:
            if req.asset_type[0] in ("", "string") or not bool(req.asset_type[0].strip()): 
                fetch_data = False      
        if  fetch_data:
            account_db={}
            for asset in req.asset_type:
                asset_data=[]
                if req.profile_id:
                    data_db1=asdbd.objects(
                        company_id=req.company_id, 
                        profile_id=req.profile_id, 
                        asset_type=asset)
                else:
                    data_db1=asdbd.objects(
                        company_id=req.company_id, 
                        asset_type=asset)
                
                # data_db=data_db1.filter(
                #     scan_completed_time__gte=prev_month_start, 
                #     scan_completed_time__lt=today)
                if data_db1:        
                    for data in data_db1:
                        data_dict = data.to_dict()  
                        count={}
                        value=data_dict['metadata'][asset]
                        for k,v in value.items():
                            count[k]=v 
                        count['date']= data_dict['scan_completed_time']#f'{date} {name}'
                        asset_data.append(count)
                    account_db[asset]=asset_data 
                    # cursor=total_count(asdbd, data_db[0].to_dict(),asset,prev_month_start, today, req.company_id,req.profile_id)
                    # account_db.update({key: val for key, val in cursor.items()})
                    ret = highest_count1(asdbd, data_db[0].to_dict(), asset, prev_month_start, today, req.company_id, req.profile_id,'1') 
                    # ret = highest_count_profile(data_db[0].to_dict(),asset,req.company_id,req.profile_id)
                    account_db.update({key: val for key, val in ret.items()})  
                logger.info(f'Fetching data for {asset} & profileid {req.profile_id}')
            return account_db
        elif fetch_data==False:
            if req.profile_id:
                    data_db=asdbd.objects.filter(
                        company_id=req.company_id, 
                        profile_id=req.profile_id)
            else:
                data_db=asdbd.objects.filter(
                    company_id=req.company_id)
            asset_list=set()    
            for data in data_db:
                data_dict=data.to_dict()
                asset_list.add(data_dict['asset_type'])     
            account_db={}
            for asset in asset_list:
                asset_data=[]
                if req.profile_id:
                    data_db1=asdbd.objects(
                        company_id=req.company_id, 
                        profile_id=req.profile_id, 
                        asset_type=asset)
                else:
                    data_db1=asdbd.objects(
                        company_id=req.company_id, 
                        asset_type=asset)
                
                # data_db=data_db1.filter(
                #     scan_completed_time__gte=prev_month_start, 
                #     scan_completed_time__lt=today)   
                if data_db1:         
                    for data in data_db1:
                        data_dict = data.to_dict()  
                        count={}
                        value=data_dict['metadata'][asset]
                        for k,v in value.items():
                            count[k]=v 
                        count['date']= data_dict['scan_completed_time']#f'{date} {name}'
                        asset_data.append(count)
                    account_db[asset]=asset_data
                    # cursor=total_count(asdbd, data_db[0].to_dict(), asset, prev_month_start, today, req.company_id, req.profile_id)
                    # account_db.update({key: val for key, val in cursor.items()})
                    ret = highest_count1(asdbd,'data_db[0].to_dict()',asset,prev_month_start,today, req.company_id,req.profile_id,'1') 
                    # ret = highest_count_profile(data_db[0].to_dict(),asset,req.company_id,req.profile_id)
                    account_db.update({key: val for key, val in ret.items()})   
                    logger.info(f'Fetching data for {asset} & profileid {req.profile_id}')
            return account_db
        elif fetch_data==None:
            return []
    

    if req.daily and not req.monthly and not req.yearly:
        # purge_data()
        today = datetime.now(timezone.utc).date()
        start_of_day = datetime.combine(today, datetime.min.time()).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
        end_of_day = datetime.combine(today, datetime.max.time()).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
        daily_db={}
        for asset in req.asset_type:
            asset_data=[]
            if req.profile_id:
                data_db = asdbd.objects.filter(
                    company_id=req.company_id, 
                    profile_id=req.profile_id,asset_type=asset,                  
                    scan_completed_time__gte=start_of_day,  
                    scan_completed_time__lt=end_of_day
                )
            else:
                data_db = asdbd.objects.filter(
                    company_id=req.company_id,
                    asset_type=asset,
                    scan_completed_time__gte=start_of_day,  
                    scan_completed_time__lt=end_of_day
                )    
            if data_db:         
                for data in data_db:
                    data_dict = data.to_dict()       
                    count={}
                    value=data_dict['metadata'][asset]
                    for k,v in value.items():
                        count[k]=v 
                    count['date']= data_dict['scan_completed_time']#f'{date} {name}'
                    asset_data.append(count)
                daily_db[asset]=asset_data  
                # cursor=total_count(asdbd, data_db[0].to_dict(), asset, start_of_day, end_of_day)
                # daily_db.update({key: val for key, val in cursor.items()})  
                ret = highest_count(asdbd, 'data_db[0].to_dict()', asset, start_of_day, end_of_day, req.company_id,req.profile_id,'day')
                daily_db.update({key: val for key, val in ret.items()})                                 
        logger.info(f'Fetching data for profile id {req.profile_id} daily basis') 
        return daily_db   
        

    if req.monthly and not req.daily and not req.yearly:                        
        purge_data_month(req.company_id,req.profile_id)  
        current_time_utc = datetime.now(timezone.utc)
        start_of_last_month_utc = (current_time_utc - relativedelta(months=1)).replace(day=1, hour=0, minute=0, second=0)
        end_of_last_month_utc = (start_of_last_month_utc - relativedelta(months=0)).replace(hour=23, minute=59, second=59)
        # end_of_last_month_utc = (current_time_utc - relativedelta(months=0)).replace(day=1, hour=0, minute=0, second=0)
        monthly_db={}
        while start_of_last_month_utc <=  current_time_utc:
            start_of_last_month_utc_str = start_of_last_month_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
            end_of_last_month_utc_str = end_of_last_month_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
            
            for asset in req.asset_type:
                if req.profile_id:
                    data_db = asdbm.objects(
                        company_id=req.company_id,
                        profile_id=req.profile_id,
                        asset_type=asset,
                        scan_completed_time__gte=start_of_last_month_utc_str,
                        scan_completed_time__lte=end_of_last_month_utc_str
                    )
                else:
                    data_db = asdbm.objects(
                        company_id=req.company_id,
                        asset_type=asset,
                        scan_completed_time__gte=start_of_last_month_utc_str,
                        scan_completed_time__lte=end_of_last_month_utc_str
                    ) #
                
                if data_db:   
                    asset_data = []
                    for data in data_db:
                        count = {}
                        data_dict = data.to_dict()
                        value = data_dict['metadata'][asset]
                        for k, v in value.items():
                            count[k] = v 
                        count['date'] = data_dict['scan_completed_time']
                        asset_data.append(count)
                           

                    monthly_db.setdefault(asset, []).extend(asset_data)
                    # for asset in req.asset_type:    
                    start_of_last_month_utc1 = (current_time_utc - relativedelta(months=1)).replace(day=1, hour=0, minute=0, second=0)
                    # end_of_last_month_utc1 = (current_time_utc - relativedelta(months=1)).replace(day=31, hour=23, minute=59, second=59)
                    
                    end_of_last_month_utc1 = current_time_utc.replace(day=1) - timedelta(days=1)
                    end_of_last_month_utc1 = end_of_last_month_utc1.replace(hour=23, minute=59, second=59)

                    start_of_last_month_utc_str1 = start_of_last_month_utc1.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
                    end_of_last_month_utc_str1 = end_of_last_month_utc1.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
                    
                    ret1 = highest_count(asdbm, 'data_db[0].to_dict()', asset, start_of_last_month_utc_str1, end_of_last_month_utc_str1, req.company_id,req.profile_id,'previous_month')

                    if ret1:
                        monthly_db.update({key: val for key, val in ret1.items()}) 
                    ############
                    start_of_last_month_utc2 = (current_time_utc - relativedelta(months=0)).replace(day=1, hour=0, minute=0, second=0)
                    end_of_last_month_utc2 = current_time_utc.replace(hour=23, minute=59, second=59)
                    
                    start_of_last_month_utc_str2 = start_of_last_month_utc2.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
                    end_of_last_month_utc_str2 = end_of_last_month_utc2.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
                    
                    ret2 = highest_count(asdbm, 'data_db[0].to_dict()', asset, start_of_last_month_utc_str2, end_of_last_month_utc_str2, req.company_id,req.profile_id,'present_month')
                    if ret2:
                        monthly_db.update({key: val for key, val in ret2.items()})      
                    # monthly_db[asset]=asset_data.copy()
                    
                    # ret = highest_count(asdbm, data_db[0].to_dict(), asset, start_of_last_month_utc_str, end_of_last_month_utc_str, req.company_id,req.profile_id)
                    # monthly_db.update({key: val for key, val in ret.items()})      
                else:
                    pass

            start_of_last_month_utc += timedelta(days=1)
            end_of_last_month_utc += timedelta(days=1)
            
          
        logger.info(f'Fetching data for profile id {req.profile_id} monthly basis') 
        return monthly_db
    

    if req.yearly and not req.monthly and not req.daily:                        
        purge_data_year(req.company_id,req.profile_id) 
        current_time_utc1 = datetime.now(timezone.utc)
        delay = timedelta(hours=5, minutes=30)
        current_time_utc = current_time_utc1 + delay
        start_of_last_year_utc = (current_time_utc - relativedelta(years=1)).replace(day=1, hour=0, minute=0, second=0)
        end_of_last_year_utc = start_of_last_year_utc.replace(hour=23, minute=59, second=59)
        yearly_db = {}

        while start_of_last_year_utc <= current_time_utc:
            # Set the end date of the current month
            end_of_last_month_utc = min(
            start_of_last_year_utc + relativedelta(months=1, days=-1, hour=23, minute=59, second=59),
            current_time_utc
            )

            start_of_last_year_utc_str = start_of_last_year_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
            end_of_last_month_utc_str = end_of_last_month_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
            for asset in req.asset_type:
                if req.profile_id:
                    data_db = asdby.objects(
                        company_id=req.company_id,
                        profile_id=req.profile_id,
                        asset_type=asset,
                        scan_completed_time__gte=start_of_last_year_utc_str,
                        scan_completed_time__lte=end_of_last_month_utc_str
                    )
                else:
                    data_db = asdby.objects(
                        company_id=req.company_id,
                        asset_type=asset,
                        scan_completed_time__gte=start_of_last_year_utc_str,
                        scan_completed_time__lte=end_of_last_month_utc_str
                    )
                if data_db:   
                    asset_data = []
                    for data in data_db:
                        count = {}
                        data_dict = data.to_dict()
                        value = data_dict['metadata'][asset]
                        for k, v in value.items():
                            count[k] = v 
                        count['date'] = data_dict['scan_completed_time']
                        asset_data.append(count)
                    
                    yearly_db.setdefault(asset, []).extend(asset_data)
                    start_of_last_year_utc2 = (current_time_utc - relativedelta(years=1)).replace(day=1, hour=0, minute=0, second=0)
                    end_of_last_year_utc2 = (current_time_utc - relativedelta(years=0)).replace(hour=23, minute=59, second=59)
                    start_of_last_year_utc_str2 = start_of_last_year_utc2.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
                    end_of_last_year_utc_str2 = end_of_last_year_utc2.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
                    ret = highest_count(asdby, 'data_db[0].to_dict()', asset, start_of_last_year_utc_str2, end_of_last_year_utc_str2, req.company_id, req.profile_id,'year')
                    yearly_db.update({key: val for key, val in ret.items()})
                else:
                    pass

            start_of_last_year_utc += relativedelta(months=1)

        logger.info(f'Fetching data for profile id {req.profile_id} yearly basis') 
        return yearly_db
    
def highest_count1(db_class,data,asset,start_date,end_date,company_id,profile_id,v):
    ret_dict={}
    # current_time_utc = datetime.now(timezone.utc)
    # for k in data['metadata'][asset].keys():
    if profile_id:
        count_high=db_class.objects(
            company_id=company_id,
            profile_id=profile_id,
            asset_type=asset,
            # scan_completed_time__gte=start_date,
            # scan_completed_time__lt=end_date
            ).order_by(f'-metadata__{asset}')#__{k}
    else: 
        count_high=db_class.objects(
            company_id=company_id,
            # profile_id=profile_id,
            asset_type=asset,
            # scan_completed_time__gte=start_date,
            # scan_completed_time__lt=end_date
            ).order_by(f'-metadata__{asset}')#__{k}   

    if count_high:
        db=count_high[0].to_dict()
        for key, val in db['metadata'][asset].items():
            ret_dict[f'count-{asset}'] = val 
        ret_dict.update({f'date-{asset}': db['scan_completed_time']})#f"{date} {name}"})        
        return ret_dict
  
def highest_count(db_class,data,asset,start_date,end_date,company_id,profile_id,v):
    ret_dict={}
    # current_time_utc = datetime.now(timezone.utc)
    # for k in data['metadata'][asset].keys():
    if profile_id:
        count_high=db_class.objects(
            company_id=company_id,
            profile_id=profile_id,
            asset_type=asset,
            scan_completed_time__gte=start_date,
            scan_completed_time__lt=end_date
            ).order_by(f'-metadata__{asset}')#__{k}
    else:
        count_high=db_class.objects(
            company_id=company_id,
            # profile_id=profile_id,
            asset_type=asset,
            scan_completed_time__gte=start_date,
            scan_completed_time__lt=end_date
            ).order_by(f'-metadata__{asset}')#__{k}    
    if count_high:
        if 'month' in v:
            db=count_high[0].to_dict()
            for key, val in db['metadata'][asset].items():
                ret_dict[f'{v}_count-{asset}'] = val 
            ret_dict.update({f'{v}_date-{asset}': db['scan_completed_time']})#f"{date} {name}"})        
            return ret_dict
        if 'year' in v:
            db=count_high[0].to_dict()
            for key, val in db['metadata'][asset].items():
                ret_dict[f'{v}_count-{asset}'] = val 
            ret_dict.update({f'{v}_date-{asset}': db['scan_completed_time']})#f"{date} {name}"})        
            return ret_dict
        if 'day' in v:
            db=count_high[0].to_dict()
            for key, val in db['metadata'][asset].items():
                ret_dict[f'{v}_count-{asset}'] = val 
            ret_dict.update({f'{v}_date-{asset}': db['scan_completed_time']})#f"{date} {name}"})        
            return ret_dict


def total_count(db_class,data,asset,start_date,end_date,company_id,profile_id):
    agg_field=data['metadata'][asset]
    group= {"_id": None}
    project={}
    for key in agg_field.keys():
        project[key]=f"$metadata.{asset}.{key}"
        group[f"total_{key}_{asset}"]={"$sum": f'${key}'}
    pipeline=[
        {"$project":project},
        {"$group":group}
        ] 
    cursor=db_class.objects(
        company_id=company_id,
        profile_id=profile_id,
        scan_completed_time__gte=start_date,
        scan_completed_time__lt=end_date
        ).aggregate(*pipeline)
    return [{kd: vd for kd, vd in doc.items() if kd != '_id'} for doc in cursor][0]


def purge_data_month(company_id,profile_id):
    today = datetime.now()
    first_day_prev_month = today.replace(day=1) - timedelta(days=1)
    prev_month_start = first_day_prev_month.replace(day=1,hour=0, minute=0, second=0)
    asdbm.objects(
        company_id=company_id,
        profile_id=profile_id,
        scan_completed_time__lte=prev_month_start, 
        ).delete()
    

def purge_data_year(company_id,profile_id):
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    twelve_months_ago = today - timedelta(days=365)
    asdby.objects(
        company_id=company_id,
        profile_id=profile_id,
        scan_completed_time__lte=twelve_months_ago
        ).delete()
