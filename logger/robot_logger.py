import logger.sql_logger as logger
import sqlite3

class RobotLogger(logger.SQLLogger):

    def __init__(self):
        super().__init__()
        self.robot_id = self.cfg["sql_database"]["robot_id"]
        self.cursor.execute("ALTER TABLE log ADD COLUMN robot_id INTEGER;")
        self.cursor.execute("ALTER TABLE local_log ADD COLUMN robot_id INTEGER;")

    def write(self, topic_name, data, source, is_keep_local_copy=False):
        super().write(topic_name, data, source, is_keep_local_copy)
        self.cursor.execute("UPDATE log SET robot_id = ? WHERE ROWID = (SELECT MAX(ROWID) FROM log)", (self.robot_id,))

        if is_keep_local_copy:
            self.cursor.execute("UPDATE local_log SET robot_id = ? WHERE ROWID = (SELECT MAX(ROWID) FROM local_log)", (self.robot_id,))

        self.database.commit()

    def __del__(self):
        if self.keepLocalCopy:
            self.backup()
            self.cursor.execute("DROP TABLE IF EXISTS local_log;")

    def backup(self):
        self.local_db_name = "local_" + self.sql_database
        self.local_db_connection = sqlite3.connect(self.local_db_name)
        self.cursor.execute("ATTACH DATABASE ? as local_db", (self.local_db_name,))
        self.cursor.execute("CREATE TABLE IF NOT EXISTS " +
                            "local_db.local_log(" +
                            "timestamp TEXT NOT NULL, " +
                            "topic_id TEXT NOT NULL, " +
                            "data BLOB NOT NULL, " +
                            "source TEXT NOT NULL, " +
                            "mismatched BOOLEAN, "+
                            "robot_id INTEGER"
                            ")"
                            )
        self.cursor.execute("CREATE TABLE IF NOT EXISTS " +
                            "local_db.topics(" +
                            "topic_id INTEGER, " +
                            "topic_name TEXT NOT NULL, " +
                            "data_type TEXT NOT NULL" +
                            ")"
                            )

        self.cursor.execute(
            "INSERT INTO local_db.local_log SELECT * FROM local_log EXCEPT SELECT * FROM local_db.local_log")
        self.cursor.execute("INSERT INTO local_db.topics SELECT * FROM topics EXCEPT SELECT * FROM local_db.topics")

        self.cursor.execute("DETACH DATABASE 'local_db'")
        self.local_db_connection.commit()
        self.database.commit()
