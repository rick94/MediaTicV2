from CSVToNeo import importCSVReactionToNeo
from neo4j.v1 import GraphDatabase, basic_auth #to connect to the database
import os, errno

driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "neo4j"))
session = driver.session()

csv_files_directory = os.path.dirname(os.path.abspath(__file__)) + '\imports'
for path, dirs, files in os.walk(csv_files_directory):
    for file in os.listdir(path):
        if file.endswith('.csv') and 'Nodes' in  file:
            edges_file = file.replace('Nodes', 'Edges')
            full_path_nodes = os.path.join(path, file)
            full_path_edges = os.path.join(path, edges_file)
            print(full_path_nodes)
            if os.path.exists(full_path_edges):
                print(full_path_edges)
                query_list = importCSVReactionToNeo(full_path_nodes, full_path_edges)
                queryNumber = len(query_list)
                queryCounter = 0
                for query in query_list:
                    session.run(query)
                    queryCounter += 1
                    print(queryCounter,' of ',queryNumber,' executed queries.')

session.close()