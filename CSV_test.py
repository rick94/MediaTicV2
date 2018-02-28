from CSVToNeo import importCSVReactionToNeo
from neo4j.v1 import GraphDatabase, basic_auth #to connect to the database
import os, errno

driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "neo4j"))
session = driver.session()

query_list = importCSVReactionToNeo('pruebaNode.csv', 'pruebaEdge.csv')
queryNumber = len(query_list)
queryCounter = 0
for query in query_list:
    session.run(query)
    queryCounter += 1
    print(queryCounter,' de ',queryNumber,' consultas ejecutadas')


session.close()