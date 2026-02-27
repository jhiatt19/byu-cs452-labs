## This script is used to query the database
import os
import psycopg2
from dotenv import load_dotenv
from datetime import timedelta


load_dotenv()
# client = OpenAI()
CONNECTION = os.getenv("TIMESCALE_SERVICE_URL") # paste connection string here or read from .env file

# response = client.embeddings.create(
#     input="that if we were to meet alien life at some point",
#     model="text-embedding-3-large"
# )


conn = psycopg2.connect(CONNECTION)
cursor = conn.cursor()

cursor.execute("""
               SELECT p.title as podcast_name, AVG(s.embedding) <-> (SELECT AVG(embedding) FROM podcast_segment 
               WHERE podcast_id = %s) as distance FROM podcast_segment as s 
               JOIN podcast p ON s.podcast_id = p.id GROUP BY p.title, p.id ORDER BY distance ASC LIMIT 6""",("VeH7qKZr0WI",))
rows = cursor.fetchall()
3
for row in rows[1:]:
    print(row)