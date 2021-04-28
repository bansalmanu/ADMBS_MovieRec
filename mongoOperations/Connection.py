import pymongo
from pymongo import MongoClient
import numpy as np

username= "manubansal"
password= "bansalmanu"
clusterUrl = "mongodb+srv://"+username+":"+password+"@cluster0.kip59.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
cluster = MongoClient(clusterUrl)

def  connectToDatabase(dbName):
    return cluster[dbName]

def openCollection(dbname,collection):
    return cluster[dbname][collection]


def correct_encoding(dictionary):
    """Correct the encoding of python dictionaries so they can be encoded to mongodb
    inputs
    -------
    dictionary : dictionary instance to add as document
    output
    -------
    new : new dictionary with (hopefully) corrected encodings"""

    new = {}
    for key1, val1 in dictionary.items():
        # Nested dictionaries
        if isinstance(val1, dict):
            val1 = correct_encoding(val1)

        if isinstance(val1, np.bool_):
            val1 = bool(val1)

        if isinstance(val1, np.int64):
            val1 = int(val1)

        if isinstance(val1, np.float64):
            val1 = float(val1)

        new[key1] = val1

    return new