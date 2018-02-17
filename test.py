from GDF_importer import parseGDF
from GdfToNeo import importGDFToNeo
from neo4j.v1 import GraphDatabase, basic_auth #to connect to the database

driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "neo4j"))
session = driver.session()

elements = parseGDF("sample.gdf")
query_list = importGDFToNeo(elements)

queryNumber = len(query_list)
queryCounter = 0
for query in query_list:
    session.run(query)
    queryCounter += 1
    print(queryCounter,' de ',queryNumber,' consultas ejecutadas')

session.close()