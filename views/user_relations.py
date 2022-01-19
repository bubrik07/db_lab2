# coding=utf-8

from models import BaseModel
from models.types import *

from .user import User


class UserRelations(BaseModel):
    """
    Database model mapping 'user_relations' table
    """

    __table__ = "user_relations"
    __schema__ = "gamers"

    owner_id = Integer("owner_id", primary_key=True, foreign_key=User.id)
    user_id = Integer("user_id", primary_key=True, foreign_key=User.id)
    is_friends = Boolean("is_friends", not_null=True)  # True -> Friends, False -> Banned
