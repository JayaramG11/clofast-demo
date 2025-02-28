from pydantic import BaseModel
class docProfileManagement(BaseModel):
    user_id:str
    profile_title:str
    profile_description:str
    defined_terms:list[dict]
    schedule_config:dict