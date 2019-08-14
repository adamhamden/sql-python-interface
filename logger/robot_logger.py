import logger.sql_logger as logger
import sqlite3

class RobotLogger(logger.SQLLogger):

    def __init__(self):
        super().__init__()
        self.robot_id = self.cfg["sql_database"]["robot_id"]
        self.cursor.execute("ALTER TABLE log ADD COLUMN robot_id INTEGER;")
        self.cursor.execute("ALTER TABLE local_log ADD COLUMN robot_id INTEGER;")
        self.cursor.execute("ALTER TABLE mismatched_type_log ADD COLUMN robot_id INTEGER;")
        self.cursor.execute("ALTER TABLE local_mismatched_type_log ADD COLUMN robot_id INTEGER;")

    def write(self, topic_name, data, is_keep_local_copy=False):
        super().write(topic_name, data, is_keep_local_copy)

        self.cursor.execute("UPDATE log set robot_id = ? WHERE timestamp  = (SELECT MAX(timestamp) FROM log)", (self.robot_id,))
        self.cursor.execute("UPDATE local_log set robot_id = ? WHERE timestamp  = (SELECT MAX(timestamp) FROM local_log)", (self.robot_id,))
        self.cursor.execute("UPDATE mismatched_type_log set robot_id = ? WHERE timestamp  = (SELECT MAX(timestamp) FROM mismatched_type_log)", (self.robot_id,))
        self.cursor.execute("UPDATE local_mismatched_type_log set robot_id = ? WHERE timestamp  = (SELECT MAX(timestamp) FROM local_mismatched_type_log)", (self.robot_id,))


    def __del__(self):
        pass

    def backup(self):
        self.local_db_name = "local_" + self.sql_database
        self.local_db_connection = sqlite3.connect(self.local_db_name)
        self.cursor.execute("ATTACH DATABASE ? as local_db", (self.local_db_name,))
        self.cursor.execute("CREATE TABLE IF NOT EXISTS " +
                            "local_db.local_log(" +
                            "timestamp TEXT NOT NULL, " +
                            "topic_id TEXT NOT NULL, " +
                            "data BLOB NOT NULL, " +
                            "robot_id INTEGER"
                            ")"
                            )
        self.cursor.execute("CREATE TABLE IF NOT EXISTS " +
                            "local_db.local_mismatched_type_log(" +
                            "timestamp TEXT NOT NULL, " +
                            "topic_id TEXT NOT NULL, " +
                            "data BLOB NOT NULL, " +
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
            "INSERT INTO local_db.local_log SELECT DISTINCT * FROM (SELECT * FROM log UNION ALL SELECT * FROM local_db.local_log)")
        self.cursor.execute(
            "INSERT INTO local_db.local_mismatched_type_log SELECT DISTINCT * FROM (SELECT * FROM local_mismatched_type_log UNION ALL SELECT * FROM local_db.local_mismatched_type_log)")
        self.cursor.execute(
            "INSERT INTO local_db.topics SELECT DISTINCT * FROM (SELECT * FROM topics UNION ALL SELECT * FROM local_db.topics)")

        self.cursor.execute("DROP TABLE IF EXISTS local_log;")
        self.cursor.execute("DROP TABLE IF EXISTS local_mismatched_type_log;")
        self.local_db_connection.commit()
        self.database.commit()

