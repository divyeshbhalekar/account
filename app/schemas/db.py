import pdb
from mongoengine import Document
from mongoengine import (StringField, DateTimeField, BooleanField, DictField, ListField,IntField)
from app.schemas.rwmodel import RWModel
from typing import List, Optional


class ServiceAccountM(Document):
    meta={
        "collection":"monthly",
        "indexes":["scan_id","company_id","profile_id"]
    }
    
    scan_id =  ListField(required=True)
    company_id = StringField(required=True)
    profile_id = StringField(required=True)
    status_code = IntField(required=False)
    asset_type = StringField(required=True)
    entity_count = IntField(required=False)
    metadata = DictField(required=False)
    failure_message = StringField(required=False)
    scan_start_time = DateTimeField(required=True)
    scan_completed_time = DateTimeField(required=True)
    
    def to_dict(self):
        return {
            "scan_id" : self.scan_id,
            "company_id": self.company_id,
            "profile_id": self.profile_id,
            "status_code" : self.status_code,
            "asset_type" : self.asset_type,
            "entity_count" : self.entity_count,
            "metadata" : self.metadata,
            "failure_message" : self.failure_message,
            "scan_start_time" : self.scan_start_time,
            "scan_completed_time" : self.scan_completed_time
        }       
    

class ServiceAccountD(Document):
    meta={
        "collection":"daily",
        "indexes":["scan_id","company_id","profile_id"]
    }
    scan_id =   ListField(required=True)
    company_id = StringField(required=True)
    profile_id = StringField(required=True)
    status_code = IntField(required=False)
    asset_type = StringField(required=True)
    entity_count = IntField(required=False)
    metadata = DictField(required=False)
    failure_message = StringField(required=False)
    scan_start_time = DateTimeField(required=True)
    scan_completed_time = DateTimeField(required=True)
    
    def to_dict(self):
        return {
            "scan_id" : self.scan_id,
            "company_id": self.company_id,
            "profile_id": self.profile_id,
            "status_code" : self.status_code,
            "asset_type" : self.asset_type,
            "entity_count" : self.entity_count,
            "metadata" : self.metadata,
            "failure_message" : self.failure_message,
            "scan_start_time" : self.scan_start_time,
            "scan_completed_time" : self.scan_completed_time
        }       

class ServiceAccountY(Document):
    meta={
        "collection":"yearly",
        "indexes":["scan_id","company_id","profile_id"]
    }
    
    scan_id =  ListField(required=True)
    company_id = StringField(required=True)
    profile_id = StringField(required=True)
    status_code = IntField(required=False)
    asset_type = StringField(required=True)
    entity_count = IntField(required=False)
    metadata = DictField(required=False)
    failure_message = StringField(required=False)
    scan_start_time = DateTimeField(required=True)
    scan_completed_time = DateTimeField(required=True)
    
    def to_dict(self):
        return {
            "scan_id" : self.scan_id,
            "company_id": self.company_id,
            "profile_id": self.profile_id,
            "status_code" : self.status_code,
            "asset_type" : self.asset_type,
            "entity_count" : self.entity_count,
            "metadata" : self.metadata,
            "failure_message" : self.failure_message,
            "scan_start_time" : self.scan_start_time,
            "scan_completed_time" : self.scan_completed_time
        }       
        

class FetchDB(RWModel):
    company_id: str
    profile_id: Optional[str]
    asset_type: List[str]
    daily: bool=False
    monthly: bool=False
    yearly: bool=False


class FetchScanSettings(RWModel):
    company_id: str
   

class AccountInCreate(FetchDB):
    pass