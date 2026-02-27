## This script is used to query the database
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

CONNECTION = os.getenv("TIMESCALE_SERVICE_URL") # paste connection string here or read from .env file

