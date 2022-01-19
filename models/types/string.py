# coding=utf-8

import typing as t
from math import inf

from ..core import Column, operator_factory
from ..expressions import RandExpr


class VarChar(Column[str]):
    """
    PostgreSQL 'VARCHAR' type
    """

    type = "VARCHAR"
    _TYPES = {str}

    # Undefined length
    _LEN: int = inf

    def __class_getitem__(cls, length: t.Optional[int] = None) -> t.Type["VarChar"]:
        """
        Set length

        Args:
            length (t.Optional[int]): VARCHAR length parameter

        Returns:
            t.Type[VarChar]: VarChar column type
        """

        if not length:
            # No length specified
            return cls

        elif not isinstance(length, int) or length <= 0:
            # Invalid length
            raise TypeError(
                f"Invalid length specified "
                f"for column of type {cls.type}")

        return t.cast(
            t.Type["VarChar"],
            type(
                f"{cls.type}_{length}",
                (cls, ),
                {
                    "type": cls.type + f"({length})",
                    "_LEN": length
                }
            )
        )

    def random(self) -> RandExpr:
        """
        Generate random expression for this column
        """

        if self.foreign_key is not None:
            # Use basic method
            return super(VarChar, self).random()

        return RandExpr(
            value=f"SUBSTRING(MD5(CAST(RANDOM() AS TEXT)) FROM 0 FOR {self._LEN})"
        )

    like = operator_factory("LIKE", "like")


__all__ = (
    "VarChar",
)
