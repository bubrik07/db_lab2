# coding=utf-8

import typing as t
from abc import ABCMeta
from decimal import Decimal

from ..core import Column
from ..expressions import RandExpr


N = t.TypeVar("N", int, float)


class _Numeric(Column[N], metaclass=ABCMeta):
    """
    Base numeric column type
    """

    _MIN: N
    _MAX: N
    _TYPES = {int, float}

    def validate(self, value: N) -> None:
        """
        Check if value is valid for numeric column type

        Args:
            value (N): Value you want to validate

        Raises:
            ValueError: If value violates NOT NULL constraint
            ValueError: If value is out of range
            TypeError: If invalid value type provided
        """

        super(_Numeric, self).validate(value)

        if not self._MIN <= self.convert(value) <= self._MAX:
            # Out of range
            raise ValueError(
                f"Invalid value for type {self.type} - "
                f"must be in range of {self._MIN} - {self._MAX}"
            )

    def random(self) -> RandExpr:
        """
        Generate random expression for this column
        """

        if self.foreign_key is not None:
            # Use basic method
            return super(_Numeric, self).random()

        # Valid
        return RandExpr(
            value=f"CAST(RANDOM() * ({self._MAX - self._MIN}) + {self._MIN} AS INTEGER)"
        )


class _Serial(_Numeric[int], metaclass=ABCMeta):
    """
    Base serial type
    """

    # Serial values starts with 1 (but actually could be 0)
    _MIN = 0


class Integer(_Numeric[int]):
    """
    PostgreSQL 'INTEGER' type
    """

    type = "INTEGER"
    _TYPES = {int}

    # 4 bytes (32 bits)
    _MIN = 0 - 2 ** 31
    _MAX = 2 ** 31 - 1


class SmallInt(_Numeric[int]):
    """
    PostgreSQL 'SMALLINT' type
    """

    type = "SMALLINT"
    _TYPES = {int}

    # 2 bytes (16 bits)
    _MIN = 0 - 2 ** 15
    _MAX = 2 ** 15 - 1


class Serial(_Serial, Integer):
    """
    PostgreSQL 'SERIAL' type
    """

    type = "SERIAL"


class SmallSerial(_Serial, SmallInt):
    """
    PostgreSQL 'SMALLSERIAL' type
    """

    type = "SMALLSERIAL"


class Numeric(_Numeric, metaclass=ABCMeta):
    """
    PostgreSQL 'NUMERIC' type
    """

    type = "NUMERIC"
    _TYPES = {int, float, Decimal}

    # Undefined scale and precision
    _SCALE: t.Optional[int] = None
    _PRECISION: t.Optional[int] = None

    _MIN = None
    _MAX = None

    def random(self) -> RandExpr:
        """
        Generate random expression for this column
        """

        if self.foreign_key is not None:
            # Use basic method
            return super(_Numeric, self).random()

        return RandExpr(
            value=f"RANDOM() * {10 ** (self._PRECISION - self._SCALE)}"
        )

    def __class_getitem__(cls, params: t.Tuple[int, int]) -> t.Type["Numeric"]:
        """
        Set precision and scale

        Args:
            params (t.Tuple[int, int]): Precision and scale

        Returns:
            t.Type[Numeric]: Numeric column type
        """

        try:
            # Unpack parameters
            precision, scale = params

        except ValueError:
            # No params
            raise TypeError(
                f"Precision and scale must be specified "
                f"for type {cls.type}"
            )

        if precision <= 0 or scale < 0:
            # Invalid paramerers
            raise ValueError(
                f"The precision must be positive, the scale zero or positive "
                f"for type {cls.type}"
            )

        elif precision < scale:
            # Invalist parameters
            raise ValueError(
                f"Precision could not be less than scale for type {cls.type}"
            )

        return t.cast(
            t.Type["Numeric"],
            type(
                f"{cls.type}_{precision}_{scale}",
                (cls, ),
                {
                    "type": cls.type + f"({precision}, {scale})",
                    "_SCALE": scale,
                    "_PRECISION": precision,
                    "_MIN": - 10 ** precision,
                    "_MAX": + 10 ** precision
                }
            )
        )


__all__ = (
    "Integer",
    "SmallInt",
    "Serial",
    "SmallSerial",
    "Numeric"
)
