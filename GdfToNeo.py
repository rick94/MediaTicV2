from Neo4JQueryBuilder import buildInsertOrUpdateNodeQuery, buildInsertOrUpdateRelationshipQuery
from GDF_importer import parseGDF

def importGDFNodeToNeo(node_list):
    query_list = []
    for element in node_list:
        if element['type'] == 'post':
            post_id = element['name']
            attribute_list = [('text', element['label']),
                              ('type_post', element['type_post']),
                              ('like_count', element['like_count']),
                              ('comment_count', element['comment_count']),
                              ('reactions_count', element['reactions_count']),
                              ('engagement', element['engagement']),
                              ('created_time', element['post_published']),
                              ('created_timeUnix', element['post_published_unix']),
                              ('shares', element['shares']),
                              ('facebook_id', element['post_id']),
                              ('post_link', element['post_link'])]
            post_node_insertion_query = buildInsertOrUpdateNodeQuery('Post', post_id, attribute_list)
            query_list.append(post_node_insertion_query)

        if element['type'] == 'user':
            user_id = element['name']
            attribute_list = [('text', element['label']),
                              ('type_post', element['type_post']),
                              ('like_count', element['like_count']),
                              ('comment_count', element['comment_count']),
                              ('reactions_count', element['reactions_count']),
                              ('engagement', element['engagement'])]
            user_node_insertion_query = buildInsertOrUpdateNodeQuery('User', user_id, attribute_list)
            query_list.append(user_node_insertion_query)

    return query_list

def importGDFEdgeToNeo(edge_list):
    query_list = []
    for element in edge_list:
       node1_id = element['node1']
       node2_id = element['node2']
       attribute_list = [('weight', element['weight']),
                         ('directed', element['directed'])]
       edge_node_insertion_query = buildInsertOrUpdateRelationshipQuery('INTERACTS_WITH', 'User', node1_id, 'Post', node2_id, attribute_list)
       query_list.append(edge_node_insertion_query)
    return query_list

def importGDFToNeo (elements):
    query_list = []
    query_list.extend(importGDFNodeToNeo(elements[0]))
    query_list.extend(importGDFEdgeToNeo(elements[1]))
    return query_list


