# coding=utf-8

import typing as t

from ..core import Column
from ..expressions import RandExpr


class Boolean(Column[bool]):
    """
    PostgreSQL BOOLEAN type
    """

    type = "BOOLEAN"
    _TYPES = {bool, int}

    @classmethod
    def convert(cls, value: t.Any) -> bool:
        """
        Convert value to BOOLEAN type
        """

        if isinstance(value, str):
            # String value
            return value.lower() not in ("0", "f", "false", "FALSE")

        else:
            # Other => use default method
            return super(Boolean, cls).convert(value)

    def random(self) -> RandExpr:
        """
        Generate random expression for this column
        """

        if self.foreign_key is not None:
            # Use basic method
            return super(Boolean, self).random()

        return RandExpr(
            value="RANDOM() > 0.5"
        )


__all__ = (
    "Boolean",
)
