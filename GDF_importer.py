# -*- coding: utf-8 -*-
import re

def readGDF(file_name):
    array = []
    with open(file_name, "r", encoding="utf8") as ins:
        for line in ins:
            array.append(line)
    return array

def getAttributes(line, header):
    line = line[len(header):]
    line = re.sub(" [a-zA-Z]+","",line)
    attributes = line.split(',')
    return attributes

def createGraphElement(line, attributes):
    element = {}
    values = line.split(',')
    if len(values) != len(attributes):
        raise Exception("Mismatch between attribute and value lists.")
    for i,attribute in enumerate(attributes):
        element[attribute] = values[i]
    return element

def parseGDF(file_name):
    edge_list = []
    node_list = []
    is_node = True
    gdf_file_data = readGDF(file_name)
    attribute_list = []
    for line in gdf_file_data:
        line = line[:-2]
        if not line.startswith("nodedef>") and not line.startswith("edgedef>"):
            if is_node:
                node_list.append(createGraphElement(line, attribute_list))
            else:
                edge_list.append(createGraphElement(line, attribute_list))
        else:
            if line.startswith("edgedef>"): is_node = False
            pos = line.find('>')
            header = line[:pos + 1]
            attribute_list = getAttributes(line, header)
    return (node_list, edge_list)



#node_header = "nodedef>name VARCHAR,label VARCHAR,type VARCHAR,type_post VARCHAR," \
#       "like_count INT,comment_count INT,reactions_count INT,engagement INT" \
#       ",post_published VARCHAR,post_published_unix INT,shares INT,post_id VARCHAR,post_link VARCHAR"
#edge_header = "edgedef>node1 VARCHAR,node2 VARCHAR,weight INT,directed BOOLEAN"
#
#node_attributes = getAttributes(node_header, "nodedef>")
#edge_attributes = getAttributes(edge_header, "edgedef>")
#
#line1 = "646d5a100359a939aa4a973007e845932fc37d3a,La viceministra de Salud  quien es médico  pasaba " \
#        "por la zona al momento del accidente y se bajó a atender a los heridos,post,link,2444,135,2444," \
#        "2673,2016-01-03T23:45:50+0000,1451864750,94,265769886798719_1081927471849619,http://www.crhoy.com/cinturon-de-seguridad" \
#        "-salvo-a-3-en-osa/,"
#line2 = "8037c9b2f8e01ffac5255d6c21f5ab982fccfffa,dbc98ba22b14e6da2f69f61b45dccf3c4e1b73c6,user,user,1,0,1,1,,,,,,"
#print(createGraphElement(line1[:-1], node_attributes))
#print(createGraphElement(line2[:-1], node_attributes))

elements = parseGDF("sample.gdf")
#print (elements[65419])