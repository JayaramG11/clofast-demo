from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Replace with your MongoDB URI and database name
MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "clofast_final"

try:
    # Establish a connection to the MongoDB server
    client = MongoClient(MONGO_URI, maxPoolSize=50, minPoolSize=10)
    db = client[DATABASE_NAME]
    print("MongoDB connection established.")
except ConnectionFailure as e:
    print(f"Failed to connect to MongoDB: {e}")


DOC_PROFILE_MANAGEMENT=db["docProfileManagement"]
PROFILE_SCHEDULER="scheduled_jobs"
DOCUMENT_MANAGEMENT=db["documentManagement"]

