from abc import ABCMeta, abstractmethod


class SqlTemplates(metaclass=ABCMeta):
    """The abstract class defined templates for SQL queries and statements.

    Abstract properties
    -------------------
    column_query: str
        SQL query template for getting database table columns by table name
    sub_tables_query -> str:
        SQL query template for getting database table names containing
        foreign keys to this table.
    search_del_query: str
        SQL query template for searching deleted rows in the database table.
    search_upsert_query: str
        SQL query template for searching updated rows in the database table.
    all_rows_query: str
        SQL query template for getting all rows from the database table.
    delete_statement: str
        SQL statement for deleting rows from the database table.
    upsert_statement: str
        SQL statement for updating and inserting rows to the database table.
    """

    @property
    @abstractmethod
    def column_query(self) -> str:
        """SQL query template for getting database table columns by table name.
        Uses the name of the database table as a placeholder 0.
        """

        pass

    @property
    @abstractmethod
    def sub_tables_query(self) -> str:
        """SQL query template for getting database table names containing
        foreign keys to this table.
        Uses the name of the database table as a placeholder 0.
        """

        pass

    @property
    @abstractmethod
    def search_del_query(self) -> str:
        """SQL query template for searching deleted rows in the database table.
        Uses the name of the primary key column as a placeholder 0.
        Uses the name of the database table as a placeholder 1.
        Uses the name of the work database as a placeholder 2.
        Uses the name of the clear database as a placeholder 3.
        """

        pass

    @property
    @abstractmethod
    def search_upsert_query(self) -> str:
        """SQL query template for searching updated rows in the database table.
        Uses the column names list as a placeholder 0.
        Uses the name of the work database as a placeholder 1.
        Uses the name of the database table as a placeholder 2.
        Uses the name of the primary key column as a placeholder 3.
        Uses the name of the update date column as a placeholder 4.
        Uses the name of the clear database as a placeholder 5.

        Warning: please, don't add a semicolon at the end of query.
        Day count condition can be added at the end of this query.
        """

        pass

    @property
    @abstractmethod
    def all_rows_query(self) -> str:
        """SQL query template for getting all rows from the database table.
        Uses the column names list as a placeholder 0.
        Uses the name of the work database as a placeholder 1.
        Uses the name of the clear database as a placeholder 3.
        """

        pass

    @property
    @abstractmethod
    def delete_statement(self) -> str:
        """SQL statement for deleting rows from the database table.
        Uses the name of the database table as a placeholder 0.
        Uses the name of the primary key column as a placeholder 1.
        Uses the row identifiers list as a placeholder 2.
        """

        pass

    @property
    @abstractmethod
    def upsert_statement(self) -> str:
        """SQL statement for updating and inserting rows to the database table.
        Uses the name of the database table as a placeholder 0.
        Uses the column names list as a placeholder 1.
        Uses the upsert values list as a placeholder 2.
        Uses the name of the primary key column as a placeholder 3.
        Uses the fields links list as a placeholder 4.
        Example: trg.column = src.column. The list must not contain a primary key.
        Uses the src.column list as a placeholder 5.
        """

        pass
