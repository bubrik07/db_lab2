# coding=utf-8

from models import BaseModel
from models.types import *

from .game_category import GameCategory
from .game_developer import GameDeveloper


class Game(BaseModel):
    """
    Database model mapping 'game' table
    """

    __table__ = "game"
    __schema__ = "gamers"

    # ID
    id = Serial("id", not_null=True, primary_key=True)

    # Foreign keys
    category_id = SmallInt("category_id", not_null=True, foreign_key=GameCategory.id)
    developer_id = Integer("developer_id", not_null=True, foreign_key=GameDeveloper.id)

    # Values
    name = VarChar[128]("name", not_null=True, unique=True, default="")
    price = Numeric[6, 2]("price")
    created_at = Date("created_at")
