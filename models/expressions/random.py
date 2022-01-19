# coding=utf-8

from dataclasses import dataclass


@dataclass(frozen=True)
class RandExpr:
    """
    Random expression class
    """

    value: str


__all__ = (
    "RandExpr",
)