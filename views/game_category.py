# coding=utf-8

from models import BaseModel
from models.types import *


class GameCategory(BaseModel):
    """
    Database model mapping 'game_category' table
    """
    
    __table__ = "game_category"
    __schema__ = "gamers"

    id = SmallSerial("id", primary_key=True)
    name = VarChar[64]("name")
