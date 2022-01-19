# coding=utf-8

import typing as t
from datetime import date, datetime

from ..core import Column
from ..expressions import RandExpr


class Date(Column[date]):
    """
    PostgreSQL 'DATE' type
    """

    type = "DATE"
    _TYPES = {date}

    @classmethod
    def convert(cls, value: t.Any) -> date:
        """
        Convert value to Date (only ISO format is accepted)
        """

        try:
            # Use base method
            return super(Date, cls).convert(value)

        except Exception:
            try:
                # Convert from isoformat
                return date.fromisoformat(value)

            except ValueError:
                # Failed
                pass

            raise TypeError(f"Invalid format for {cls.type}")

    def random(self) -> RandExpr:
        """
        Generate random expression for this column
        """

        if self.foreign_key is not None:
            # Use basic method
            return super(Date, self).random()

        return RandExpr(
            value="CAST(NOW() - RANDOM() * (INTERVAL '20 YEARS') AS DATE)"
        )


class TimeStampTZ(Column[datetime]):
    """
    PostgreSQL 'TIMESTAMP WITH TIME ZONE' type
    """

    type = "TIMESTAMP WITH TIME ZONE"
    _TYPES = {datetime}

    @classmethod
    def convert(cls, value: t.Any) -> datetime:
        """
        Convert value to Date (only ISO format is accepted)
        """

        try:
            # Use base method
            return super(TimeStampTZ, cls).convert(value)

        except Exception:
            for converter in (
                lambda v: datetime.fromisoformat(value),
                lambda v: datetime.strptime(value + "00", "%Y-%m-%d %H:%M:%S.%f%z"),
                lambda v: datetime.strptime(value + "00", "%Y-%m-%d %H:%M:%S%z")
            ):
                try:
                    # Convert using current converted
                    return converter(value)

                except ValueError:
                    # Failed
                    pass

            # Failed to convert with any of converters
            raise TypeError(f"Invalid format for {cls.type}")

    def random(self) -> RandExpr:
        """
        Generate random expression for this column
        """

        if self.foreign_key is not None:
            # Use basic method
            return super(TimeStampTZ, self).random()

        return RandExpr(
            value="NOW() - RANDOM() * (INTERVAL '20 YEARS')"
        )


__all__ = (
    "Date",
    "TimeStampTZ"
)