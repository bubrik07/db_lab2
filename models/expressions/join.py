# coding=utf-8

import typing as t
from abc import ABCMeta

if t.TYPE_CHECKING:
    # Should define types
    from ..core import Column, MetaModel

    Table = MetaModel

else:
    # Use string types
    Table = "MetaModel"


# Used types
Joined = t.Container[Table]
JoinFunc = t.Callable[[Joined], t.Tuple[str, Table]]


class JoinFactory(metaclass=ABCMeta):
    """
    Base JOIN clause factory
    """

    def __init__(self, keyword: str) -> None:
        """
        Initialize JOIN clause factory
        """

        self._KEYWORD = keyword

    def __call__(self, first: "Column", second: "Column") -> JoinFunc:
        """
        Join two columns

        Args:
            first (Column): First column you want to join
            second (Column): Second column you want to join

        Returns:
            JoinFunc: Join function
        """

        def join_clause(joined: Joined) -> t.Tuple[str, Table]:
            """
            Perform join of specified columns if given 'joined' tables are already joined

            Args:
                joined (Joined): Set of already joined tables

            Returns:
                t.Tuple[str, Table]: Join expression + joined table
            """

            if first.table in joined:
                # Joining table from second column
                table = second.table

            elif second.table in joined:
                # Joining table from first column
                table = first.table

            else:
                # Unknown tables
                raise ValueError("Can't join - unknown tables provided")

            # Get columns comparator
            comparator, *_ = first == second
            query = (
                f"{self._KEYWORD} {table}"  # type: ignore # TODO: FIX
                f"\n\tON {comparator}"
            )

            # Join query
            return query, table

        return join_clause


join = JoinFactory("JOIN")
inner_join = JoinFactory("INNER JOIN")
left_outer_join = JoinFactory("LEFT OUTER JOIN")
right_outer_join = JoinFactory("RIGHT OUTER JOIN")
full_outer_join = JoinFactory("FULL OUTER JOIN")


__all__ = (
    "JoinFunc",
    "JoinFactory",
    "join",
    "inner_join",
    "left_outer_join",
    "right_outer_join",
    "full_outer_join"
)
