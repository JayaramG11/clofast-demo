from fastapi import FastAPI, Header
from DB import *
from models import *
from utility import *
import logging
import uuid
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
from apscheduler.triggers.cron import CronTrigger
import pymongo

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
# Configure CORS
origins = [
    "http://localhost:3000",  # Frontend URL
    # Add other allowed origins as needed
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

jobstores = {
    'default': MongoDBJobStore(database=DATABASE_NAME, collection=PROFILE_SCHEDULER, client=client)
}
scheduler = BackgroundScheduler(jobstores=jobstores)
scheduler.start()

def process_documents(profileId: str):
    logging.info(f"Entered into the running background tasks with profileId: {profileId}")
    result = DOCUMENT_MANAGEMENT.find({"profileId": profileId}, {"_id": 0})
    result_list = list(result)
    for doc in result_list:
        logger.info(f"Processing the document {doc}")
        # Need to write the code here after implementing the function
        # Need to update the status of the document
    print("hello")


@app.post("/insert/documents/profileId")
def add_documents_profile(
    user_id: str,
    profile_title: str,
    profile_description: str,
    defined_terms: list[dict[str, str]],
    schedule_config: dict[str, str],  # Renamed from `scheduler` to `schedule_config`
):
    try:
        profile_uuid = str(uuid.uuid4())
        if schedule_config["frequency"] != "custom":
            schedule_date = schedule_config["date_str"]
            cron_expression = generate_cron_expression(schedule_date, schedule_config["frequency"])
        else:
            cron_expression = schedule_config["cron_expression"]

        payload = {
            "createdTime": datetime.now(),
            "updatedTime": "",
            "userId": user_id,
            "profileId": profile_uuid,
            "profileTitle": profile_title,
            "profileDescription": profile_description,
            "definedTerms": defined_terms,
            "scheduler": schedule_config,
            "cronExpression": cron_expression,
            "version": 0,
            "total_documents":0,
            "active_documents":0,
            "inactive_documents":0,
            "status": "active",
            
        }
        DOC_PROFILE_MANAGEMENT.insert_one(payload)
        logger.info(f"Payload was inserted successfully: {payload}")

        # Use the global `scheduler` object
        job = scheduler.add_job(
            process_documents,
            trigger=CronTrigger.from_crontab(cron_expression),
            args=[profile_uuid],
            id=profile_uuid,
        )

        logger.info(f"Scheduled job for profile {user_id} (ID: {profile_uuid}) with cron expression {cron_expression}")
        if "_id" in payload:
            payload["_id"]= str(payload["_id"])

        return payload

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        return {"error": str(e)}


@app.get("/get/user/profiles")
def get_user_profiles(userId:str):
    """
    Retrieve all profiles associated with a user.
    
    """
    res= DOC_PROFILE_MANAGEMENT.find({"userId": userId}, {"_id": 0}) 
    result=list(res)
    return result
@app.get("/get/profiles")
def get_profile(condition="all"):
    """get all the profiles """
    if condition == "all":
        res=DOC_PROFILE_MANAGEMENT.find({}, {"_id": 0})
    elif condition == "active":
        res=DOC_PROFILE_MANAGEMENT.find({"status": "active"}, {"_id": 0})
    elif condition == "inactive":
        res=DOC_PROFILE_MANAGEMENT.find({"status": "inactive"}, {"_id": 0})
    result=list(res)
    return result
@app.get("/get/all/documents/associated/to/the/profile")
def get_all_documents_associated_to_the_profile(profileId:str,condition="all"):
    """
    Retrieve all documents associated with a profile.

    """
    if condition == "all":
        res= DOCUMENT_MANAGEMENT.find({"profileId": profileId}, {"_id": 0}) 
    elif condition=="unprocessed":
        res=DOCUMENT_MANAGEMENT.find({"profileId": profileId,"status":"unprocessed"}, {"_id": 0}) 
    elif condition=="processed":
        res=DOCUMENT_MANAGEMENT.find({"profileId": profileId,"status":"processed"}, {"_id": 0})
    result=list(res)
    return result

@app.get("/get/status/of/all/profiles")
def get_status_of_all_profiles():
    res= DOC_PROFILE_MANAGEMENT.find({}, {"_id": 0}) 
    result=list(res)
    active_count=0
    inactive_count=0
    

    for i in result:
        if i["status"] == "active":
            active_count=active_count+1    
        else:
            inactive_count=inactive_count+1
        
    return {"message-key":"status-of-all-profiles","data":{"active_count":active_count,"inactive_count":inactive_count,"total_count":active_count+inactive_count}}

@app.get("/get/particular/profile")
def get_particular_profile(profileId:str):
    res= DOC_PROFILE_MANAGEMENT.find({"profileId":profileId}, {"_id": 0}) 
    result=list(res)
    return result
@app.delete("/delete/profile")
def delete_profile(profileId:str):
    res= DOC_PROFILE_MANAGEMENT.delete_one({"profileId":profileId})
    return {"message-key":"profile-deleted"}

@app.get("/get/sotred/data/based/on/conditions")
def get_sotred_data_based_on_conditions(sort="createdTimeDSC",filter="all"):
    if filter=="all":
        if sort=="createdTimeDSC":
            res=DOC_PROFILE_MANAGEMENT.find({}, {"_id": 0}).sort("createdTime", pymongo.DESCENDING)
        elif sort=="createdTimeASC":
            res=DOC_PROFILE_MANAGEMENT.find({}, {"_id": 0}).sort("createdTime", pymongo.ASCENDING)
        elif sort=="ProfileNameDSC":
            res=DOC_PROFILE_MANAGEMENT.find({}, {"_id": 0}).sort("profileTitle", pymongo.DESCENDING)
        elif sort=="ProfileNameASC":
            res=DOC_PROFILE_MANAGEMENT.find({}, {"_id": 0}).sort("profileTitle", pymongo.ASCENDING)
        elif sort=="noOfDocumentsDSC":
            res=DOC_PROFILE_MANAGEMENT.find({}, {"_id": 0}).sort("total_documents", pymongo.DESCENDING)
        elif sort=="noOfDocumentsASC":
            res=DOC_PROFILE_MANAGEMENT.find({}, {"_id": 0}).sort("total_documents", pymongo.ASCENDING)
    elif filter=="active":
        if sort=="createdTimeDSC":
            res=DOC_PROFILE_MANAGEMENT.find({"status":"active"}, {"_id": 0}).sort("createdTime", pymongo.DESCENDING)
        elif sort=="createdTimeASC":
            res=DOC_PROFILE_MANAGEMENT.find({"status":"active"}, {"_id": 0}).sort("createdTime", pymongo.ASCENDING)
        elif sort=="ProfileNameDSC":
            res=DOC_PROFILE_MANAGEMENT.find({"status":"active"}, {"_id": 0}).sort("profileTitle", pymongo.DESCENDING)
        elif sort=="ProfileNameASC":
            res=DOC_PROFILE_MANAGEMENT.find({"status":"active"}, {"_id": 0}).sort("profileTitle", pymongo.ASCENDING)
        elif sort=="noOfDocumentsDSC":
            res=DOC_PROFILE_MANAGEMENT.find({"status":"active"}, {"_id": 0}).sort("total_documents", pymongo.DESCENDING)
        elif sort=="noOfDocumentsASC":
            res=DOC_PROFILE_MANAGEMENT.find({"status":"active"}, {"_id": 0}).sort("total_documents", pymongo.ASCENDING)
    elif filter=="inactive":
        if sort=="createdTimeDSC":
            res=DOC_PROFILE_MANAGEMENT.find({"status":"inactive"}, {"_id": 0}).sort("createdTime", pymongo.DESCENDING)
        elif sort=="createdTimeASC":
            res=DOC_PROFILE_MANAGEMENT.find({"status":"inactive"}, {"_id": 0}).sort("createdTime", pymongo.ASCENDING)
        elif sort=="ProfileNameDSC":
            res=DOC_PROFILE_MANAGEMENT.find({"status":"inactive"}, {"_id": 0}).sort("profileTitle", pymongo.DESCENDING)
        elif sort=="ProfileNameASC":
            res=DOC_PROFILE_MANAGEMENT.find({"status":"inactive"}, {"_id": 0}).sort("profileTitle", pymongo.ASCENDING)
        elif sort=="noOfDocumentsDSC":
            res=DOC_PROFILE_MANAGEMENT.find({"status":"inactive"}, {"_id": 0}).sort("total_documents", pymongo.DESCENDING)
        elif sort=="noOfDocumentsASC":
            res=DOC_PROFILE_MANAGEMENT.find({"status":"inactive"}, {"_id": 0}).sort("total_documents", pymongo.ASCENDING)
    result=list(res)
    return result