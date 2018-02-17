from GDF_importer import parseGDF
from GdfToNeo import importGDFToNeo
from neo4j.v1 import GraphDatabase, basic_auth #to connect to the database
import os, errno


driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "neo4j"))
session = driver.session()

gdf_files_directory = os.path.dirname(os.path.abspath(__file__)) + '\imports'
for path, dirs, files in os.walk(gdf_files_directory):
    for file in os.listdir(path):
        if file.endswith('.gdf'):
            gdf_file = os.path.join(path, file)
            elements = parseGDF(gdf_file)
            query_list = importGDFToNeo(elements)
            queryNumber = len(query_list)
            queryCounter = 0
            for query in query_list:
                session.run(query)
                queryCounter += 1
                print(queryCounter,' de ',queryNumber,' consultas ejecutadas')

session.close()