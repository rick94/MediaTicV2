import csv
import hashlib
from Neo4JQueryBuilder import buildInsertOrUpdateNodeQuery, buildInsertOrUpdateRelationshipQuery

reaction_dic = {}

def importCSVReactionNodeToNeo(file_name):
    global reaction_dic
    query_list = []
    with open (file_name, newline='') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        current_post_id = ""
        for row in csv_reader:
            if row['type'] != 'user':
                post_id = hide_id(row['node_id'])
                current_post_id = post_id
                attribute_list = [('type', row['type']),
                                  ('link', row['link']),
                                  ('name', row['name']),
                                  ('message', row['message']),
                                  ('created_time', row['created_time'])]
                post_node_insertion_query = buildInsertOrUpdateNodeQuery('Post', post_id, attribute_list)
                query_list.append(post_node_insertion_query)
            else:
                user_id =  hide_id(row['node_id'])
                reaction_key = user_id + '_' + current_post_id
                reaction_dic[reaction_key] = row['reaction_type']
                attribute_list = [('type', row['type'])]
                user_node_insertion_query = buildInsertOrUpdateNodeQuery('User', user_id, attribute_list)
                query_list.append(user_node_insertion_query)
        return query_list


def importCSVReactionEdgeToNode(file_name):
    global reaction_dic
    query_list = []
    with open (file_name, newline='') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            node1_id = hide_id(row['source'])
            node2_id = hide_id(row['target'])
            reaction_key = node1_id + '_' + node2_id
            attribute_list = [('reaction_type', reaction_dic[reaction_key])]
            edge_node_insertion_query = buildInsertOrUpdateRelationshipQuery('REACTS_TO', 'User', node1_id, 'Post',
                                                                             node2_id, attribute_list)
            query_list.append(edge_node_insertion_query)
        return query_list

def importCSVReactionToNeo (node_file_name, edge_file_name):
    global reaction_dic
    query_list = []
    query_list.extend(importCSVReactionNodeToNeo(node_file_name))
    query_list.extend(importCSVReactionEdgeToNode(edge_file_name))
    reaction_dic = {}
    return query_list


def hide_id(id):
    id_as_bytes = str.encode(id)
    hash_object = hashlib.md5(id_as_bytes)
    id_hash = hash_object.hexdigest()
    return id_hash