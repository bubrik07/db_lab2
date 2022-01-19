# coding=utf-8

import typing as t
from csv import DictReader
from pathlib import Path

from views import *
from models import *


def _sep(length: int = 32) -> None:
    """
    Print separator
    """

    print("=" * length)


def _subsep(length: int = 32) -> None:
    """
    Print sub-separator
    """

    print("-" * length)


def _table_selector() -> t.Optional[t.Type[BaseModel]]:
    """
    Ask user for table

    Returns:
        t.Optional[t.Type[BaseModel]]: Selected table
    """

    print(
        "Select table:",
        "",
        *(f"- {number}. {table}" for number, table in enumerate(_tables)),
        "- X. Cancel action",
        "",
        sep="\n"
    )
    table_number = input(f"Type a number (0-{len(_tables) - 1}): ").strip().upper()

    # Validate

    if table_number == "X":
        # Cancel
        return None

    elif table_number not in map(str, range(len(_tables))):
        # Invalid
        print("Unknown table identifier specified")
        _subsep()
        _table_selector()

    return _tables[int(table_number)]


def _fill() -> None:
    """
    Fill database tables with initial data
    """

    data_path = Path() / "data"

    for table in _tables:
        with open(data_path / (table.__table__ + ".csv"), "rt") as table_file:
            # Initialize reader
            data_reader = DictReader(table_file)

            # Insert values
            table.insert(*(table(**row) for row in data_reader))

    print("Success")


def _banned_users() -> None:
    """
    Select banned users
    """

    users = User.select(
        filters=(
            User.is_banned == True,
        )
    )

    User.print(users)


def _number_of_friends() -> None:
    """
    Select number of friends for specified <user.id>
    """

    user_id = input("Type <user.id>: ")

    users = UserRelations.select(
        joins=(
            join(UserRelations.owner_id, User.id)
        ),
        filters=(
            User.id == user_id,
            UserRelations.is_friends == True
        )
    )

    print(f"Number of friends of <user.id={user_id}>: {len(users)}")


def _purchase_game() -> None:
    """
    Purchase <game.id> for specified <user.id>
    """

    game_id = input("Type <game.id>: ")
    user_id = input("Type <user.id>: ")

    # Initialize purchase
    purchase = UserPurchase(
        user_id=user_id,
        game_id=game_id
    )

    # Insert into db
    UserPurchase.insert(purchase)

    # Success
    print(f"Successfully purchased <game.id={game_id}> for <user.id={user_id}>")


def _unblock_users() -> None:
    """
    Unblock all users for <user.id>
    """

    user_id = input("Type <user.id>: ")

    deleted = UserRelations.delete(
        filters=(
            UserRelations.owner_id == user_id,
            UserRelations.is_friends == False
        )
    )

    print(f"{deleted} users was unblocked for <user.id={user_id}>")


def _total_price() -> None:
    """
    Get total price for all existing games
    """

    # Select all prices
    games = Game.select(
        Game.price
    )

    # Calculate total
    price = sum(game.price for game in games)

    print(f"Total price of all games: {price:.2f}")


def _get_developer_of_game() -> None:
    """
    Select developer who created <game.name>
    """

    game_name = input("Type <game.name>: ")

    # Query with join
    developer = GameDeveloper.select(
        joins=(
            join(Game.developer_id, GameDeveloper.id)
        ),
        filters=(
            Game.name.like(game_name)
        )
    )

    if not developer:
        # Not found
        print("Developer not found for this game")

    else:
        # Output
        GameDeveloper.print(developer)


def _change_price() -> None:
    """
    Change price for specified <game.id> to <game.price>
    """

    game_id = input("Type <game.id>: ")
    game_price = input("Type <game.price>: ")

    try:
        # Update query
        Game.update(
            {
                "price": game_price
            },
            filters=(
                Game.id == game_id
            )
        )

    except Exception as error:
        # Failed
        print(f"Failed to update price: {error}")

    else:
        # Success
        print("Successfuly updated")


def _users_register_date() -> None:
    """
    Select users who registered earlier than specified date (ISO8601-formatted)
    """

    date = input("Type <user.created_at> (ISO-8601): ")
    
    users = User.select(
        filters=(
            User.created_at <= date
        )
    )

    User.print(users)


def _generate_table() -> None:
    """
    Generate random values for table
    """

    table = _table_selector()

    if table is None:
        # Canceled
        return None

    # Select total number
    number = input("Type <number>: ")

    # Generate random table rows
    tables = table.random(int(number))

    table.print(tables)


def _select_table() -> None:
    """
    Select everything from table
    """

    table = _table_selector()

    if table is None:
        # Canceled
        return None

    # Select & output
    table.print(
        table.select()
    )


# Existing options
_tables = [
    GameCategory,
    GameDeveloper,
    Game,
    User,
    UserPurchase,
    UserRelations
]
_actions = {
    "0": ("Fill database tables with initial data", _fill),
    "1": ("Select banned users", _banned_users),
    "2": ("Select number of friends of <user.id>", _number_of_friends),
    "3": ("Purchase <game.id> for <user.id>", _purchase_game),
    "4": ("Unblock all users for <user.id>", _unblock_users),
    "5": ("Get total price for all existing games", _total_price),
    "6": ("Select developer who created <game.name>", _get_developer_of_game),
    "7": ("Change price for <game.id> to <game.price>", _change_price),
    "8": ("Select users who registered earlier than <user.created_at>", _users_register_date),
    "R": ("Generate <number> of random values for <table.name>", _generate_table),
    "S": ("Select everything from <table.name>", _select_table)
}


def listen() -> None:
    """
    Process user commands
    """

    # Get action

    _sep()
    print(
        "Select an action you want to do:",
        "",
        *(f"- {number}. {value}" for number, (value, _) in _actions.items()),
        "",
        sep="\n"
    )
    action_number = input(f"Type an action identifier: ").strip().upper()
    _subsep()

    # Validate

    if action_number not in _actions:
        # Invalid
        print("Unknown action identifier specified")
        return None

    # Action from list of actions
    func = _actions[action_number][1]
    func()
