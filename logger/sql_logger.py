#!/usr/bin/env python3

import sqlite3
import yaml
class SQLLogger:

    def __init__(self):

        self.cfg = None
        with open("config.yml", 'r') as ymlfile:
            self.cfg = yaml.safe_load(ymlfile)

        self.sql_database =  self.cfg["sql_database"]["database_name"]
        self.keepLocalCopy = self.cfg["sql_database"]["keep_local_copy"]

        self.database = sqlite3.connect(self.sql_database)
        self.cursor = self.database.cursor()

        self.cursor.execute("CREATE TABLE IF NOT EXISTS " +
                            "log(" +
                            "timestamp TEXT NOT NULL, " +
                            "topic_id TEXT NOT NULL, " +
                            "data BLOB NOT NULL, " +
                            "source TEXT NOT NULL, "+
                            "mismatched BOOLEAN"
                            ")"
                            )

        self.cursor.execute("CREATE TABLE IF NOT EXISTS " +
                            "local_log(" +
                            "timestamp TEXT NOT NULL, " +
                            "topic_id TEXT NOT NULL, " +
                            "data BLOB NOT NULL, " +
                            "source TEXT NOT NULL, "+
                            "mismatched BOOLEAN"
                            ")"
                            )

        self.cursor.execute("CREATE TABLE IF NOT EXISTS " +
                            "topics(" +
                            "topic_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                            "topic_name TEXT NOT NULL, " +
                            "data_type TEXT NOT NULL" +
                            ")"
                            )

    def __del__(self):
        if self.keepLocalCopy:
            self.backup()
            self.cursor.execute("DROP TABLE IF EXISTS local_log;")

    def write(self, topic_name, data, source, is_keep_local_copy=False):

        self.cursor.execute("SELECT * from topics WHERE topic_name == ?", (topic_name,))
        topic_matches = self.cursor.fetchall()
        if len(topic_matches) != 1:
            raise ValueError("Attempting to write to an invalid topic name")

        topic_id = topic_matches[0][0]
        topic_data_type = topic_matches[0][2]
        insert_data_type = str(type(data).__name__)

        is_mismatched = 0
        if topic_data_type != insert_data_type:
            is_mismatched = 1

        values_to_insert = (topic_id, data, source, is_mismatched)
        self.cursor.execute("INSERT INTO log(timestamp, topic_id, data, source, mismatched) VALUES (DATETIME('now'),?,?,?,?)", values_to_insert)

        if is_keep_local_copy:
            self.cursor.execute("INSERT INTO local_log(timestamp, topic_id, data, source, mismatched) VALUES (DATETIME('now'),?,?,?,?)", values_to_insert)


        self.database.commit()


    def add_topic(self, topic_name, data_type):

        self.cursor.execute("SELECT * from topics WHERE topic_name == ?", (topic_name,))
        topic_matches = self.cursor.fetchall()
        if len(topic_matches) != 0:
            raise ValueError("Attempting add an existing topic")

        self.cursor.execute("INSERT INTO topics VALUES (NULL, ?, ?)", (topic_name, data_type,))

    def backup(self):

        self.local_db_name = "local_" + self.sql_database
        self.local_db_connection = sqlite3.connect(self.local_db_name)
        self.cursor.execute("ATTACH DATABASE ? as local_db", (self.local_db_name,))
        self.cursor.execute("CREATE TABLE IF NOT EXISTS " +
                            "local_db.local_log(" +
                            "timestamp TEXT NOT NULL, " +
                            "topic_id TEXT NOT NULL, " +
                            "data BLOB NOT NULL, " +
                            "source TEXT NOT NULL, "+
                            "mismatched BOOLEAN"
                            ")"
                            )
        self.cursor.execute("CREATE TABLE IF NOT EXISTS " +
                            "local_db.topics(" +
                            "topic_id INTEGER, " +
                            "topic_name TEXT NOT NULL, " +
                            "data_type TEXT NOT NULL" +
                            ")"
                            )

        self.cursor.execute("INSERT INTO local_db.local_log SELECT * FROM local_log EXCEPT SELECT * FROM local_db.local_log")
        self.cursor.execute("INSERT INTO local_db.topics SELECT * FROM topics EXCEPT SELECT * FROM local_db.topics")

        self.cursor.execute("DETACH DATABASE 'local_db'")
        self.local_db_connection.commit()
        self.database.commit()



