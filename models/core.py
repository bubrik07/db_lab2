# coding=utf-8

import psycopg2
import typing as t
from re import match
from time import perf_counter
from tabulate import tabulate
from datetime import datetime
from inspect import isabstract
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field

from .expressions import JoinFunc, RandExpr


T = t.TypeVar("T")
V = t.TypeVar("V")
Q = t.TypeVar("Q")

Literal = t.Union[str, T]
Operand = t.Union["Column[T]", t.Optional[T]]
Row = t.Dict[str, V]
Rows = t.List[Row[V]]

# WHERE
WhereExpr = t.Tuple[str, t.Optional[t.Dict[str, T]]]
_WhereMethod = t.Callable[["Column[T]", Operand], WhereExpr[T]]

# Expressions
Joins = t.Iterable[JoinFunc]
Wheres = t.Iterable[WhereExpr[T]]

# Database parameters
_connection = None
cursor = None
ECHO = True


def connect(
        host: str = "localhost",
        port: t.Union[str, int] = 5432,
        username: t.Optional[str] = None,
        password: t.Optional[str] = None,
        database: t.Optional[str] = None,
        echo: bool = False,
        **kwargs: t.Any
) -> None:
    """
    Connect to database with specified parameters
    """

    global _connection, cursor, ECHO

    print("Connecting to database...")
    start_time = perf_counter()

    try:
        # Start connection
        _connection = psycopg2.connect(
            f"host={host} " +
            f"port={port} " +
            (f" user={username}" if username else "") +
            (f" password={password}" if password else "") +
            (f" dbname={database}" if database else "")
        )
        _connection.autocommit = True

        # Initialize cursor
        cursor = _connection.cursor()

        # Set echo parameter
        ECHO = echo

    except Exception as error:
        # Failed
        raise ConnectionError(f"Connection failed: {error}")

    else:
        # Success
        print(f"Successfully connected ({perf_counter() - start_time:.3f} s)")


@dataclass
class Query(t.Generic[Q], metaclass=ABCMeta):
    """
    Base database query class
    """

    parameters: t.Dict[str, t.Any] = field(default_factory=dict)
    tables: t.List["MetaModel"] = field(default_factory=list)

    @abstractmethod
    def _render(self) -> str: ...

    def execute(self) -> Q:
        """
        Execute query
        """

        if isabstract(self):
            # Abstract query cannot be a valid query
            raise NotImplementedError(f"Cannot execute abstract query {self!r}")

        elif cursor is None:
            # No connection
            raise ConnectionError("Database is not connected")

        if ECHO:
            # Log query
            print()
            print("[", datetime.utcnow(), "]", sep="")
            print(self._query)
            print(self.parameters)
            print()

        # Execute query
        cursor.execute(self._query, self.parameters)

        # Retrieve data
        return cursor.fetchall()

    def __str__(self) -> str:
        """
        Return query
        """

        return self._query

    @property
    def table(self) -> t.Optional["MetaModel"]:
        """
        Get primary table
        """

        if self.tables:
            # Tables provided
            return self.tables[0]

        return None

    @classmethod
    def _iter(cls, value: T) -> t.Union[T, t.List[T]]:
        """
        Convert value to iterable (if it is not an iterable)

        Args:
            value (T): Value you want to convert (whether iterable or no)

        Returns:
            t.Union[T, t.List[T]]: Iterable value
        """

        if not hasattr(value, "__iter__"):
            # Not an iterable
            return [value]

        # Get value types
        _type = None

        for val in value:
            if _type is None:
                # First
                _type = type(val)

            elif _type != type(val):
                # Invalid type => not an list of values
                return [value]

        return value

    def __post_init__(self) -> None:
        """
        Initialize query after init & fix some issues
        """

        self.tables = self._iter(self.tables)
        self._query = self._render()


@dataclass
class _FilterQuery(Query[Q], metaclass=ABCMeta):
    """
    Database filter query class
    """

    joins: t.List[JoinFunc] = field(default_factory=list)
    filters: t.List[str] = field(default_factory=list)
    order_by: t.Optional["Column"] = None
    ascending: bool = True
    limit: t.Optional[int] = None
    offset: t.Optional[int] = None

    def __post_init__(self):
        """
        Initialize query after init & fix some issues
        """

        self.joins = self._iter(self.joins)
        self.filters = self._iter(self.filters)

        super(_FilterQuery, self).__post_init__()

    @abstractmethod
    def _render(self) -> str:
        """
        Render filter query part
        """

        query: str = ""
        expression: str

        for join in self.joins or []:
            # Generate join expr
            expression, table = join(self.tables)

            # Add joined table to set
            self.tables.append(table)

            query += f"\n{expression} "

        if self.filters:
            # Add WHERE clause
            query += "\nWHERE\n\t"
            query_filters: t.List[str] = []

            for expression, parameter in self.filters:
                # Add filters
                query_filters.append(expression)

                if parameter is not None:
                    # Add parameter variable
                    self.parameters.update(parameter)

            query += "\n\tAND\n\t".join(query_filters)

        if self.order_by:
            # Add ORDER BY clause

            if self.order_by.table not in self.tables:
                # Unknown column
                raise ValueError("Can't order by unknown column")

            query += f"\nORDER BY {self.order_by} " + "ASC" if self.ascending else "DESC" + " "

        if self.limit:
            # Add LIMIT clause

            if not isinstance(self.limit, int) or self.limit < 0:
                # Invalid limit value
                raise ValueError("Query limit must be non-negative integer value")

            query += f"\nLIMIT {self.limit} "

        if self.offset:
            # Add offset clause

            if not isinstance(self.offset, int) or self.offset < 0:
                # Invalid offset value
                raise ValueError("Query offset must be non-negative integer value")

            query += f"\nOFFSET {self.offset} "

        return query.strip()


@dataclass
class _UpdaterQuery(_FilterQuery[int], metaclass=ABCMeta):
    """
    Database updater query class
    """

    def execute(self) -> int:
        """
        Execute updater query

        Returns:
            int: Number of updated rows
        """

        return len(
            super(_UpdaterQuery, self).execute()
        )


@dataclass
class _ValuesQuery(Query[Q], metaclass=ABCMeta):
    """
    Database values (asignment) query class
    """

    values: t.List["BaseModel"] = field(default_factory=list)

    def __post_init__(self):
        """
        Initialize query after init & fix some issues
        """

        self.values = self._iter(self.values)

        super(_ValuesQuery, self).__post_init__()

    @abstractmethod
    def _render(self) -> str:
        """
        Render values (asignment) query class
        """

        query = ""

        inserted, *redundant = self.tables

        if redundant:
            # Can insert only in single table
            raise ValueError("INSERT can be performed only for single table")

        # Get inserted columns
        columns = list(inserted.columns)

        if columns and self.values:
            # Values exist
            query += "\nVALUES"

            for number, value in enumerate(self.values):
                if not isinstance(value, inserted):
                    # Unknown table
                    raise TypeError(f"Incompatible inserted table - {value.__table__}")

                query += "\n\t("

                for column in columns:
                    for value_column in value.columns:  # type: ignore
                        if value_column.name == column.name:
                            # Column found
                            break

                    else:
                        # Not found ???
                        raise ValueError(f"Column {column.name!r} not found in {value!r}")

                    if isinstance(getattr(value, column.name), RandExpr):
                        # Insert directly into query
                        query += f"{getattr(value, column.name).value}, "

                    else:
                        # # Check if ok
                        # column.validate(value)

                        # Get value name
                        name = f"{column.table.__table__}_{column.name}_{number}"

                        self.parameters[name] = getattr(value, column.name)
                        query += f"%({name})s, "

                query = query[:-2]  # Remove last separator
                query += "),"

            query = query[:-1]  # Remove last separator

        return query


@t.final
@dataclass
class SelectQuery(_FilterQuery[T], t.Generic[T]):
    """
    SELECT query
    """

    columns: t.List["Column"] = field(default_factory=list)

    def __post_init__(self):
        """
        Initialize query after init & fix some issues
        """

        self.columns = self._iter(self.columns)

        super(SelectQuery, self).__post_init__()

    def _render(self) -> str:
        """
        Render selection query
        """

        query = ""
        selected = ",\n\t".join(map(str, self.columns or self.table.columns))

        if selected:
            # Initialize select query
            query += (
                f"SELECT\n"
                f"\t{selected}\n"
                f"FROM {self.table}\n"  # type: ignore
            )

            query += super(SelectQuery, self)._render()

        return query.strip()

    def execute(self) -> t.List[T]:
        """
        Execute selection query
        """

        results: t.List[T] = []
        columns = list(self.columns or self.table.columns)

        for row in super(SelectQuery, self).execute():
            # Initialize table
            table = self.table()

            for number, value in enumerate(row):
                # Set table column value
                setattr(table, columns[number].name, value)

            results.append(table)

        return results


@t.final
@dataclass
class InsertQuery(_ValuesQuery[T], t.Generic[T]):
    """
    INSERT query
    """

    upsert: bool = False

    def _render(self) -> str:
        """
        Render insertion query
        """

        query = super(InsertQuery, self)._render()

        if query:
            # Add INSERT clause
            query = (
                f"INSERT INTO {self.table}\n"  # type: ignore
                f"\t({', '.join((column.name for column in self.table.columns))}) "
                + query
            )

            if self.upsert:
                # Update on conflict
                query += (
                    f"\nON CONFLICT"
                    f"\n\t({', '.join((column.name for column in self.table.primary_keys))})"
                    f"\nDO UPDATE {self.table}"  # type: ignore
                    f"\nSET\n\t"
                    + "\n\t".join((f"\"{column.name}\" = {column}" for column in self.table.columns))
                )

            query += (
                f"\nON CONFLICT ({', '.join((column.name for column in self.table.primary_keys))}) DO NOTHING"
                f"\nRETURNING\n"  # Return values to update in case of autoincrement sequences
                f"\t({', '.join((column.name for column in self.table.columns))})"  # ^^^ or default values
            )

        return query.strip()


@t.final
@dataclass
class UpdateQuery(_UpdaterQuery):
    """
    UPDATE query
    """

    values: t.Dict[str, t.Any] = field(default_factory=dict)

    def _render(self) -> str:
        """
        Render update query
        """

        query = ""
        updated_column = ""

        if self.values:
            # Update columns
            query = (
                f"UPDATE {self.table}\n"  # type: ignore
                f"SET"
            )

            for updated_column, value in self.values.items():
                for column in self.table.columns:
                    if column.name == updated_column:
                        # Found column
                        break

                else:
                    # Not found ???
                    raise ValueError(
                        f"Column {updated_column!r} not found "
                        f"in table '{self.table.__table__}'"  # type: ignore
                    )

                # Check if ok
                column.validate(value)

                # Get value name
                name = f"{column.table.__table__}_{column.name}"  # type: ignore

                # Add column to list
                query += f"\n\t{column.name} = %({name})s,"
                self.parameters[name] = value

            query = query[:-1] + "\n"
            query += super(UpdateQuery, self)._render()

            # Return to get total number
            query += f"\nRETURNING {updated_column}"

        return query


@t.final
@dataclass
class DeleteQuery(_UpdaterQuery):
    """
    DELETE query
    """

    def _render(self) -> str:
        """
        Render delete query
        """

        if self.table:
            # Add DELETE clause
            return (
                f"DELETE FROM "
                f"{self.table}\n"  # type: ignore
                + super(DeleteQuery, self)._render() +
                f"\nRETURNING *"
            )

        else:
            # Empty
            return ""


class _classproperty(object):
    """
    Combine classmethod & property
    """

    def __init__(self, fget):
        self.fget = fget

    def __get__(self, owner_self, owner_cls):
        return self.fget(owner_cls)


def operator_factory(operator: str, name: str) -> _WhereMethod[T]:
    """
    Create operator (comparator)

    Args:
        operator (str): Operator you want to use
        name (str): Operator name (for formatting)

    Returns:
        Operator: Operator method
    """

    def comparator(self: "Column[T]", other: Operand) -> WhereExpr:
        """
        Operator method
        """

        if isinstance(other, Column):
            # Compare with column
            value, param = other, None

        else:
            # Compare with literal value => validate
            self.validate(other)

            # Format parameter
            _name = f"{self.table.__table__}_{self.name}_{name}"  # type: ignore # TODO: FIX
            value, param = f"%({_name})s", {_name: other}

        return f"{self} {operator} {value}", param

    return comparator


class Column(t.Generic[T], metaclass=ABCMeta):
    """
    Table column class
    """

    type: str  # PostgreSQL type name
    _TYPES: t.Set[t.Type]  # Compatible types

    __slots__ = (
        "name",
        "primary_key",
        "unique",
        "not_null",
        "table",
        "foreign_key",
        "_default",
    )

    def __init__(
            self,
            name: str,
            primary_key: bool = False,
            unique: bool = False,
            not_null: bool = False,
            default: T = None,
            foreign_key: t.Optional["Column[T]"] = None
    ):
        """
        Initialize table column

        Args:
            name (str): Column name (only letter, digits and undescores)
            primary_key (bool): If True, column is represented as a primary key
            unique (bool): If True, column is represented as a unique
            not_null (bool): If True, column is represented as NOT NULL constraint
            default (T): If specified, then this value will be set if no value given
            foreign_key (t.Optional(Column[T])): Specify referenced column if it is a foreign key

        Raises:
            ValueError: If name contains invalid characters (not in range [A-Za-z0-9_])
            TypeError: If type is unknown (not an attribute of 'Types' subclass)
        """

        if not match(r"[A-Za-z0-9_]", name):
            # Invalid name
            raise ValueError(f"Invalid column name {name!r}")

        self.name = name
        self.foreign_key = foreign_key

        # Constraints
        self.primary_key = bool(primary_key)

        self.unique = bool(unique) or self.primary_key  # True if is a primary key
        self.not_null = bool(not_null) or self.primary_key  # True if is a primary key

        # Default value
        self._default = default

    def __get__(self, instance: t.Optional["BaseModel"], owner: "MetaModel") -> t.Union[T, "Column[T]"]:
        """
        Get column instance or value

        Args:
            instance (t.Optional[BaseModel]): If specified, then value of this column returned
            owner (MetaModel): Model class

        Returns:
            t.Union[T, Column[T]]: Get either column value or column instance
        """

        if isinstance(instance, BaseModel):
            # Instance specified
            return getattr(instance, "_" + self.name, self.default)

        else:
            # Return column with table
            self.table = owner
            return self

    def __set__(self, instance: "BaseModel", value: T) -> None:
        """
        Set column value

        Args:
            instance (BaseModel): Column table instance
            value (T): Column value you want to set
        """

        setattr(instance, "_" + self.name, self.convert(value))

    def __hash__(self) -> int:
        """
        Get column hash
        """

        return hash(str(self))

    @classmethod
    def convert(cls, value: t.Any) -> T:
        """
        Convert specified value to column type

        Args:
            value (t.Any): Value you want to convert

        Raises:
            TypeError: If value is not compatible with type
        """

        if type(value) in {RandExpr, *cls._TYPES}:
            # Valid type
            return value

        for _type in cls._TYPES:
            try:
                # Convert value to type
                return _type(value)

            except Exception:
                # Failed
                pass

        raise TypeError(
            f"Could not convert value of type {type(value)} "
            f"to be compatible with column of type {cls.type}"
        )

    def validate(self, value: T) -> None:
        """
        Check if value is valid for column type

        Args:
            value (T): Value you want to validate

        Raises:
            ValueError: If value violates NOT NULL constraint
            TypeError: If invalid value type provided
        """

        if self.not_null and value is None:
            # NOT NULL violation
            raise ValueError(
                "Invalid value - "
                "NOT NULL constraint violation"
            )

        try:
            # Check if compatible
            self.convert(value)

        except (TypeError, ValueError):
            # Invalid type
            raise TypeError(
                f"Invalid value for type {self.type!r} provided - "
                f"{value!r} ({type(value)})"
            )

    @property
    def default(self) -> T:
        """
        Get default column value
        """

        if callable(self._default):
            # Factory
            return self._default()

        else:
            # Value
            return self._default

    @property
    def expression(self) -> str:
        """
        Get column SQL expression
        """

        return (
            str(self)
            + f" {self.type}"
            + f" UNIQUE" if self.unique else ""
            + f" NOT NULL" if self.not_null else ""
            + f" DEFAULT {self.default}" if self.default is not None else ""
        )

    def __str__(self) -> str:
        """
        Get column name

        Returns:
            str: Column name
        """

        return f"\"{self.table.__table__}\".\"{self.name}\""  # type: ignore # TODO: FIX

    @abstractmethod
    def random(self) -> t.Optional[RandExpr]:
        """
        Generate random expression for this column
        """

        if self.foreign_key is not None:
            # Values subquery
            return RandExpr(
                value=(
                    f"(SELECT {self.foreign_key.name}"
                    f" FROM {self.foreign_key.table}"
                    f" ORDER BY RANDOM() LIMIT 1)"
                )
            )

    __eq__ = operator_factory("=", "eq")
    __ne__ = operator_factory("!=", "ne")
    __lt__ = operator_factory("<", "lt")
    __gt__ = operator_factory(">", "gt")
    __le__ = operator_factory("<=", "le")
    __ge__ = operator_factory(">=", "ge")


class MetaModel(type, t.Generic[T]):
    """
    Table model metaclass
    """

    @property
    def columns(cls) -> t.Generator[Column, None, None]:
        """
        Get model columns

        Yields:
            Column: Model columns
        """

        for attr in dir(cls):
            if isinstance(getattr(cls, attr), Column):
                # Column type
                yield getattr(cls, attr)

    @property
    def primary_keys(cls) -> t.Generator[Column, None, None]:
        """
        Get model primary keys

        Yieds:
            Column: Model columns defined as primary keys
        """

        for column in cls.columns:
            if column.primary_key:
                # Defined as primary key
                yield column

    def print(cls, tables: t.List["BaseModel[T]"]) -> None:
        """
        Print specified list of tables

        Args:
            tables (t.List[BaseModel[T]]): List of tables you want to print
        """

        if not all(map(lambda table: type(table) is cls, tables)):
            # Invalid tables
            raise TypeError("Cannot print different tables")

        print(tabulate(
            [
                [column.name for column in cls.columns],  # Header
                *[[getattr(table, column.name) for column in cls.columns] for table in tables]  # Rows
            ],
            tablefmt="grid",
            headers="firstrow"
        ))

    def select(
            cls,
            *columns: Column,
            joins: t.Optional[Joins] = None,
            filters: t.Optional[Wheres] = None,
            order_by: t.Optional[Column] = None,
            ascending: bool = True,
            limit: t.Optional[int] = None,
            offset: t.Optional[int] = None
    ) -> t.List["MetaModel[T]"]:
        """
        Execute SELECT query on table

        Args:
            *columns (Column): Specify column you want to select. If none provided, selects all
            joins (t.Optional[Joins]): Selection joins (JOIN ON clause)
            filters (t.Optional[Wheres]): Selection filters (WHERE clause)
            order_by (t.Optional[Column]): If specified, sorts results by this column (ORDER BY clause)
            ascending (bool): If 'order_by' specified, sets sort order (ASC/DESC clause)
            limit (t.Optional[int]): If specified, limits number of rows with this number (LIMIT clause)
            offset (t.Optional[int]): If specified, skips this number of first rows (OFFSET clause)

        Returns:
            Rows: List of selected rows
        """

        query = SelectQuery(
            tables=[cls],
            columns=list(columns),
            joins=joins or [],
            filters=filters or [],
            order_by=order_by,
            ascending=ascending,
            limit=limit,
            offset=offset
        )

        return query.execute()

    def insert(
            cls,
            *values: "BaseModel[T]",
            upsert: bool = False
    ) -> None:
        """
        Execute INSERT query on table

        Args:
            *values (BaseModel[T]): Specify tables you want to insert
            upsert (bool): If True, updates conflicting rows
        """

        query = InsertQuery(
            tables=[cls],
            values=list(values),
            upsert=upsert
        )

        # Insert values and get them (returned as str)
        inserted = [
            value[1:-1].replace("\"", "").replace("'", "").split(",")
            for value, *_ in query.execute()
        ]

        if len(inserted) < len(values):
            # Not all has been inserted => update by primary keys
            for insert in inserted:
                for table in values:
                    for index, column in enumerate(cls.columns):
                        if column.primary_key:
                            if str(getattr(table, column.name)) != insert[index]:
                                # Not this row
                                break

                    else:
                        # Found row
                        break

                else:
                    # Row not found?
                    continue

                for index, column in enumerate(cls.columns):
                    # Update table value
                    setattr(table, column.name, insert[index])

        else:
            # All has been inserted => update by order
            for index, column in enumerate(cls.columns):
                for number, table in enumerate(values):
                    # Update table value
                    setattr(table, column.name, inserted[number][index])

    def update(
            cls,
            values: t.Dict[str, t.Any],
            filters: t.Optional[Wheres] = None,
            order_by: t.Optional[Column] = None,
            ascending: bool = True,
            limit: t.Optional[int] = None,
            offset: t.Optional[int] = None,
    ) -> int:
        """
        Execute UPDATE query on table

        Args:
            values (t.Dict[str, t.Any]): Mapping with name of updated columns and new values
            filters (t.Optional[Wheres]): Selection filters (WHERE clause)
            order_by (t.Optional[Column]): If specified, sorts results by this column (ORDER BY clause)
            ascending (bool): If 'order_by' specified, sets sort order (ASC/DESC clause)
            limit (t.Optional[int]): If specified, limits number of rows with this number (LIMIT clause)
            offset (t.Optional[int]): If specified, skips this number of first rows (OFFSET clause)
        """

        query = UpdateQuery(
            tables=[cls],
            values=values,
            filters=filters or [],
            order_by=order_by,
            ascending=ascending,
            limit=limit,
            offset=offset
        )

        return query.execute()

    def delete(
            cls,
            filters: t.Optional[Wheres] = None,
            order_by : t.Optional[Column] = None,
            ascending: bool = True,
            limit: t.Optional[int] = None,
            offset: t.Optional[int] = None,
    ) -> int:
        """
        Execute DELETE query on table

        Args:
            filters (t.Optional[Wheres]): Selection filters (WHERE clause)
            order_by (t.Optional[Column]): If specified, sorts results by this column (ORDER BY clause)
            ascending (bool): If 'order_by' specified, sets sort order (ASC/DESC clause)
            limit (t.Optional[int]): If specified, limits number of rows with this number (LIMIT clause)
            offset (t.Optional[int]): If specified, skips this number of first rows (OFFSET clause)
        """

        query = DeleteQuery(
            tables=[cls],
            filters=filters or [],
            order_by=order_by,
            ascending=ascending,
            limit=limit,
            offset=offset
        )

        return query.execute()

    def __str__(self) -> str:
        """
        Get table name with specified schema name

        Returns:
            str: "<schema>"."<table>"-formatted string
        """

        return (
            f"\"{getattr(self, '__schema__')}\"."
            f"\"{getattr(self, '__table__')}\""
        )

    @abstractmethod
    def random(cls, number: int = 1) -> t.List["BaseModel[T]"]:
        """
        Generate given number of random rows for this table

        Args:
            number (int): Number of rows you want to generate
        """

        # Generate random tables
        tables = [cls._random() for _ in range(number)]

        # Insert them
        cls.insert(*tables)

        return tables


class BaseModel(t.Generic[T], metaclass=MetaModel[T]):
    """
    Base table model
    """

    __slots__ = ()

    __table__: str
    __schema__: str = "public"

    def __init__(self, **columns) -> None:
        """
        Initialize table model
        """

        for column, value in columns.items():
            # Set column value
            setattr(self, column, value)

    @classmethod
    @abstractmethod
    def _random(cls) -> "BaseModel[T]":
        """
        Generate table with random expression
        """

        return cls(**{
            column.name: column.random() for column in cls.columns
        })

    columns = _classproperty(lambda cls: cls.columns)
    primary_keys = _classproperty(lambda cls: cls.primary_keys)


__all__ = (
    "connect",
    "Query",
    "SelectQuery",
    "InsertQuery",
    "UpdateQuery",
    "DeleteQuery",
    "BaseModel",
    "Column"
)