from datetime import datetime
from dbconnector import DbConnector


LOGGER_DICT_STUB = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "default": {
            "level": "CRITICAL",
            "class": "logging.StreamHandler"
        }
    },
    "loggers": {
        "": {
            "handlers": ["default"],
            "level": "CRITICAL",
            "propagate": True
        }
    }
}
IS_CONNECTED = DbConnector().check_and_close_connection()
WORK_DB_NAME = "UnitTest_DbTable_Work"
CLEAR_DB_NAME = "UnitTest_DbTable_Clear"
TABLE_NAME = "dbo.test"
PRIMARY_KEY_COL = "test_id"
INT_COL = "test_int"
FLOAT_COL = "test_float"
STR_COL = "test_str"
UPDATE_DT_COL = "test_upddt"
COLUMNS = [PRIMARY_KEY_COL, INT_COL, FLOAT_COL, STR_COL, UPDATE_DT_COL]
STR_COLUMNS = ",".join(COLUMNS)
LINK_COLUMNS = (',\n' + ' ' * 12).join(["trg.{0} = src.{0}".format(col)
                                        for col in COLUMNS
                                        if col != PRIMARY_KEY_COL])
INS_COLUMNS = ",".join(["src.{0}".format(col) for col in COLUMNS])
DT = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
DT_STR = DT.isoformat(sep=' ', timespec='milliseconds')
SUB_TABLES = ["dbo.sub1", "dbo.sub2"]

CREATE_SUB_TABLES_SCRIPTS = [
    f"""CREATE TABLE dbo.sub1(
        [sub1_id] [bigint] IDENTITY(1,1) NOT NULL,
        [{PRIMARY_KEY_COL}] [bigint] NULL,
     CONSTRAINT [PK_sub1] PRIMARY KEY CLUSTERED ([sub1_id] ASC)
    ) ON [PRIMARY];
    ALTER TABLE dbo.sub1 WITH NOCHECK ADD CONSTRAINT [fk_1]
    FOREIGN KEY([{PRIMARY_KEY_COL}])
    REFERENCES {TABLE_NAME} ([{PRIMARY_KEY_COL}]);
    ALTER TABLE dbo.sub1 CHECK CONSTRAINT [fk_1];""",
    f"""CREATE TABLE dbo.sub2(
        [sub2_id] [bigint] IDENTITY(1,1) NOT NULL,
        [{PRIMARY_KEY_COL}] [bigint] NULL,
     CONSTRAINT [PK_sub2] PRIMARY KEY CLUSTERED ([sub2_id] ASC)
    ) ON [PRIMARY];
    ALTER TABLE dbo.sub2 WITH NOCHECK ADD CONSTRAINT [fk_2]
    FOREIGN KEY([{PRIMARY_KEY_COL}])
    REFERENCES {TABLE_NAME} ([{PRIMARY_KEY_COL}]);
    ALTER TABLE dbo.sub2 CHECK CONSTRAINT [fk_2];"""
]

DROP_SUB_TABLES_SCRIPTS = ["DROP TABLE dbo.sub1;", "DROP TABLE dbo.sub2;"]

CREATE_TABLE_SCRIPT = f"""
CREATE TABLE {TABLE_NAME}(
    [{PRIMARY_KEY_COL}] [bigint] IDENTITY(1,1) NOT NULL,
    [{INT_COL}] [bigint] NULL,
    [{FLOAT_COL}] [float] NULL,
    [{STR_COL}] [varchar](100) NULL,
    [{UPDATE_DT_COL}] [datetime] NULL,
 CONSTRAINT [PK_test] PRIMARY KEY CLUSTERED ([{PRIMARY_KEY_COL}] ASC)
) ON [PRIMARY];"""

CREATE_DB_SCRIPT = f"""
CREATE DATABASE [{WORK_DB_NAME}];
CREATE DATABASE [{CLEAR_DB_NAME}];"""

INIT_SCRIPT = f"""
USE [{WORK_DB_NAME}];
{CREATE_TABLE_SCRIPT}

USE [{CLEAR_DB_NAME}];
{CREATE_TABLE_SCRIPT}"""

TRUNCATE_SCRIPT = f"""
TRUNCATE TABLE {WORK_DB_NAME}.{TABLE_NAME};
TRUNCATE TABLE {CLEAR_DB_NAME}.{TABLE_NAME};
"""

DROP_SCRIPT = f"""
USE [master];
DROP DATABASE [{WORK_DB_NAME}];
DROP DATABASE [{CLEAR_DB_NAME}];"""
