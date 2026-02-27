## This script is used to create the tables in the database

import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

CONNECTION = os.getenv("TIMESCALE_SERVICE_URL") # paste connection string here or read from .env file

# need to run this to enable vector data type
CREATE_EXTENSION = "CREATE EXTENSION vector"

# TODO: Add create table statement
CREATE_PODCAST_TABLE = """
    CREATE TABLE podcast (
        id TEXT PRIMARY KEY,
        title TEXT  
    )
"""
# TODO: Add create table statement
CREATE_SEGMENT_TABLE = """
    CREATE TABLE podcast_segment (
        id TEXT PRIMARY KEY,
        start_time INTERVAL,
        end_time INTERVAL,
        content TEXT,
        embedding VECTOR(128),
        podcast_id TEXT REFERENCES podcast(id)
    )
"""

conn = psycopg2.connect(CONNECTION)
# TODO: Create tables with psycopg2 (example: https://www.geeksforgeeks.org/executing-sql-query-with-psycopg2-in-python/)

cursor = conn.cursor()

try:
    cursor.execute(CREATE_PODCAST_TABLE)
    cursor.execute(CREATE_SEGMENT_TABLE)
except psycopg2.Error as e:
    conn.rollback()

conn.commit()
conn.close()

