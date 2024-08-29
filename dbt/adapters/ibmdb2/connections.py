from contextlib import contextmanager
from enum import Enum

from dataclasses import dataclass

import dbt_common.exceptions
from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.contracts.connection import AdapterResponse

# from dbt.adapters.contracts.connection import Connection
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.events.logging import AdapterLogger

from typing import (
    # Type,
    # Iterable,
    Optional,
)

# import ibm_db
import ibm_db_dbi

logger = AdapterLogger("IBM DB2")


@dataclass
class AuthType:
    BASIC_AUTH: str = "basic-auth"
    KERBEROS: str = "kerberos"


class SysType:
    LUW: str = "luw"
    ZOS: str = "z/os"


@dataclass
class IBMDB2Credentials(Credentials):
    running_on: str = SysType.LUW
    authentication: str = AuthType.BASIC_AUTH
    dsn: Optional[str] = ""
    host: Optional[str] = ""
    database: str = "default_database"
    schema: str = "default_schema"
    user: Optional[str] = ""
    password: Optional[str] = ""
    port: Optional[int] = 50000
    protocol: Optional[str] = "TCPIP"
    extra_connect_opts: Optional[str] = ""

    @property
    def type(self):
        return "ibmdb2"

    @property
    def unique_field(self) -> str:
        if self.authentication == AuthType.BASIC_AUTH:
            return self.host
        if self.authentication == AuthType.KERBEROS:
            return self.dsn
        return ""

    def _connection_keys(self):
        if self.authentication == AuthType.BASIC_AUTH:
            return (
                "host",
                "database",
                "schema",
                "user",
                "password",
                "port",
                "protocol",
                "extra_connect_opts",
            )
        if self.authentication == AuthType.KERBEROS:
            return (
                "dsn",
                "database",
                "schema",
            )
        return (None,)


class IBMDB2ConnectionManager(SQLConnectionManager):
    TYPE = "ibmdb2"

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except ibm_db_dbi.DatabaseError as exc:
            self.release()
            logger.debug("ibm_db_dbi error: {}".format(str(exc)))
            logger.debug("Error running SQL: {}".format(sql))
            raise dbt_common.exceptions.DbtDatabaseError(str(exc))
        except Exception as exc:
            self.release()
            logger.debug("Error running SQL: {}".format(sql))
            logger.debug("Rolling back transaction.")
            raise dbt_common.exceptions.DbtRuntimeError(str(exc))

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        def connect():
            credentials = connection.credentials
            if credentials.authentication == AuthType.BASIC_AUTH:
                con_str = (
                    f"DATABASE={credentials.database}"
                    f";HOSTNAME={credentials.host}"
                    f";PORT={credentials.port}"
                    f";PROTOCOL={credentials.protocol}"
                    f";UID={credentials.user}"
                    f";PWD={credentials.password}"
                )
                if credentials.extra_connect_opts:
                    con_str += f";{credentials.extra_connect_opts}"
            elif credentials.authentication == AuthType.KERBEROS:
                con_str = (
                    f"{credentials.dsn};"
                    f"DATABASE={credentials.database}"
                    f"SCHEMA={credentials.schema}"
                )
            else:
                raise ValueError(f"Not a valid auth type: {credentials.authentication}")

            handle = ibm_db_dbi.connect(con_str, "", "")
            handle.set_autocommit(False)

            return handle

        retryable_exceptions = [
            ibm_db_dbi.OperationalError,
        ]

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=3,
            retry_timeout=5,
            retryable_exceptions=retryable_exceptions,
        )

    @classmethod
    def cancel(self, connection):
        connection_name = connection.name

        logger.info("Cancelling query '{}' ".format(connection_name))

        try:
            connection.handle.close()
        except Exception as e:
            logger.error("Error closing connection for cancel request")
            raise Exception(str(e))

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:

        message = "OK"
        rows = cursor.rowcount

        return AdapterResponse(_message=message, rows_affected=rows)

    def add_begin_query(self):
        pass
