# coding=utf-8

from models import BaseModel
from models.types import *


class GameDeveloper(BaseModel):
    """
    Database model mapping 'game_developer' table
    """

    __table__ = "game_developer"
    __schema__ = "gamers"

    id = Serial("id", primary_key=True)
    name = VarChar[128]("name")
