# coding=utf-8

from models import BaseModel
from models.types import *

from .user import User
from .game import Game


class UserPurchase(BaseModel):
    """
    Database model mapping 'user_purchase' table
    """
    
    __table__ = "user_purchase"
    __schema__ = "gamers"

    user_id = Integer("user_id", primary_key=True, foreign_key=User.id)
    game_id = Integer("game_id", primary_key=True, foreign_key=Game.id)
    created_at = TimeStampTZ("created_at", default=datetime.datetime.utcnow())
