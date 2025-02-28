from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.mongodb import MongoDBJobStore
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import logging
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(title="Profile Scheduler", description="Schedule jobs for profiles with cron expressions.")

# MongoDB configuration
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "profile_scheduler"
client = MongoClient(MONGO_URI, maxPoolSize=10)  # Connection pooling with max 10 connections
db = client[DB_NAME]
profiles_collection = db["profiles"]

# APScheduler configuration with MongoDB job store
jobstores = {
    'default': MongoDBJobStore(database=DB_NAME, collection='scheduled_jobs', client=client)
}
scheduler = BackgroundScheduler(jobstores=jobstores)
scheduler.start()

# Pydantic models
class Document(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    content: str

class Profile(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    cron_expression: str
    documents: List[Document]

class ProfileResponse(BaseModel):
    id: str
    name: str
    cron_expression: str
    documents: List[Document]

class JobResponse(BaseModel):
    job_id: str
    profile_id: str
    next_run_time: Optional[datetime]

# Helper function to process documents
def process_documents(profile_id: str):
    """Process all documents associated with a profile."""
    profile = profiles_collection.find_one({"id": profile_id})
    if not profile:
        logger.error(f"Profile {profile_id} not found.")
        return

    logger.info(f"Processing documents for profile {profile['name']} (ID: {profile_id})")
    for doc in profile["documents"]:
        logger.info(f"Processing document {doc['id']}: {doc['content']}")

# API Endpoints
@app.post("/profiles/", response_model=ProfileResponse, status_code=status.HTTP_201_CREATED)
async def create_profile(profile: Profile):
    """Create a new profile with associated documents and a cron expression."""
    try:
        # Insert profile into MongoDB
        profile_dict = profile.dict()
        profiles_collection.insert_one(profile_dict)

        # Schedule a job for the profile
        job = scheduler.add_job(
            process_documents,
            trigger=CronTrigger.from_crontab(profile.cron_expression),
            args=[profile.id],
            id=profile.id,
        )

        logger.info(f"Scheduled job for profile {profile.name} (ID: {profile.id}) with cron expression {profile.cron_expression}")
        return profile_dict
    except DuplicateKeyError:
        raise HTTPException(status_code=400, detail="Profile with this ID already exists.")

@app.get("/profiles/", response_model=List[ProfileResponse])
async def get_profiles():
    """Retrieve all profiles."""
    profiles = list(profiles_collection.find())
    return profiles

@app.get("/jobs/", response_model=List[JobResponse])
async def get_scheduled_jobs():
    """Retrieve all scheduled jobs."""
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "job_id": job.id,
            "profile_id": job.args[0],
            "next_run_time": job.next_run_time,
        })
    return jobs

@app.delete("/profiles/{profile_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_profile(profile_id: str):
    """Delete a profile and its associated scheduled job."""
    profile = profiles_collection.find_one_and_delete({"id": profile_id})
    if not profile:
        raise HTTPException(status_code=404, detail="Profile not found.")

    # Remove the scheduled job
    scheduler.remove_job(profile_id)
    logger.info(f"Deleted profile {profile['name']} (ID: {profile_id}) and its scheduled job.")
    return None

# Shutdown event
@app.on_event("shutdown")
def shutdown_event():
    """Cleanup tasks when the application shuts down."""
    scheduler.shutdown()
    client.close()
    logger.info("Application shutdown complete.")