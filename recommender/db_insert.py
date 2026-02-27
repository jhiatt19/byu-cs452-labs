## This script is used to insert data into the database
import os
import json
from copy import deepcopy
from dotenv import load_dotenv
from datasets import load_dataset
import pandas as pd
from path import Path
import psycopg2
from utils import fast_pg_insert

load_dotenv()

CONNECTION = os.getenv("TIMESCALE_SERVICE_URL")


docFolder = Path('documents')
embedFolder = Path('embedding')
docDataFrame = []
embedDataFrame = []



# TODO: Read the embedding files
for embed in os.listdir(embedFolder):
    filePath = os.path.join(embedFolder,embed)
    embed_temp = pd.read_json(filePath, lines=True)
    #embed_temp['source_file'] = embed

    docPath = 'batch_request_' + filePath[10:]
    docFile = os.path.join(docFolder,docPath)
    doc_temp = pd.read_json(docFile,lines=True)


    bothDataFrames = pd.merge(embed_temp,doc_temp, on='custom_id', how='outer')
    podcastDataFrame = deepcopy(bothDataFrames)
    podcastDataFrame['id'] = podcastDataFrame['body'].str['metadata'].str['podcast_id']
    podcastDataFrame['title'] = podcastDataFrame['body'].str['metadata'].str['title']
    podcastDataFrame = podcastDataFrame[['id','title']]
    bothDataFrames['id'] = bothDataFrames['custom_id']
    bothDataFrames['start_time'] = bothDataFrames['body'].str['metadata'].str['start_time']
    bothDataFrames['end_time'] = bothDataFrames['body'].str['metadata'].str['stop_time']
    bothDataFrames['content'] = bothDataFrames['body'].str['input']
    bothDataFrames['embedding'] = bothDataFrames['response'].str['body'].str['data'].str[0].str['embedding']
    bothDataFrames['podcast_id'] = bothDataFrames['body'].str['metadata'].str['podcast_id']
    bothDataFrames = bothDataFrames.drop(columns=['error','url','custom_id','method','response','body'])
    podcastDataFrame = podcastDataFrame.drop_duplicates(subset='id')

    #check for duplicates
    conn = psycopg2.connect(CONNECTION)
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM podcast")

    currentIDs = cursor.fetchall()
    replaceIDs = []
    for id in currentIDs:
        replaceIDs.append(id[0])
    if currentIDs is not None:
        podcastDataFrame = podcastDataFrame[~podcastDataFrame['id'].isin(replaceIDs)]
    

    fast_pg_insert(podcastDataFrame,CONNECTION,"podcast",['id','title'])
    fast_pg_insert(bothDataFrames,CONNECTION,"podcast_segment",['id','start_time','end_time', 'content', 'embedding', 'podcast_id'])
 