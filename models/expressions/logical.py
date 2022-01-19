# coding=utf-8

import typing as t


# Define expression type
T = t.TypeVar("T")
Expression = t.Tuple[str, t.Optional[t.Dict[str, T]]]


def _logical_expr_factory(operator: str) -> t.Callable[..., Expression]:
    """
    Create logical expression func for specified operator
    """

    def logical_expr(*expressions: Expression) -> Expression:
        """
        Combine expressions with AND operation
        """

        operands: t.List[str] = []
        parameters: t.Dict[str, T] = {}

        for expression, param in expressions:
            # Add to expression
            operands.append(expression)
            parameters.update(param or {})

        return f"({(' ' + operator + ' ').join(operands)})", parameters

    return logical_expr


# Define functions
and_ = _logical_expr_factory("AND")
or_ = _logical_expr_factory("OR")


__all__ = (
    "Expression",
    "and_",
    "or_"
)
