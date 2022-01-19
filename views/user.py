# coding=utf-8

from models import BaseModel
from models.types import *


class User(BaseModel):
    """
    Database model mapping 'user' table
    """

    __table__ = "user"
    __schema__ = "gamers"

    id = Serial("id", primary_key=True)
    email = VarChar[128]("email", unique=True, not_null=True)
    password_hash = VarChar[128]("password_hash", not_null=True)
    created_at = TimeStampTZ("created_at")
    is_banned = Boolean("is_banned", default=False)
