import pandas as pd
import matplotlib.pyplot as plt
from openpyxl import load_workbook
from sqlalchemy import create_engine
import numpy as np
import networkx as nx


def get_edges(paths: list) -> list:
    res = []
    if len(paths)==1:
        res = paths
    else:
        for i,x in enumerate(paths):
            if (i<len(paths)-1):
                res.append((paths[i],paths[i+1]))
    return res


def clear_path(path: list) -> list:
    res = []
    res.append(path[0])
    for i,x in enumerate(path):
        if (i!=len(path)-1):
            if(path[i] != path[i+1]):
                res.append(path[i+1])

    return res


RFM_query = '''
select
       listagg(cluster,';' ) within group (order by date) as cluster_path,
       user_id
from growth.rfm_history
group by user_id;
'''

# Datawarehouse connection
f = open('C:\\Users\\gabri\\Documents\\Queries\\db_klarprod_connection.txt', 'r')
postgres_str = f.read()
f.close()
cnx = create_engine(postgres_str)

# RFM query
rfm_paths = pd.read_sql_query(RFM_query, cnx)

# Create list from string
rfm_paths['path_list'] = rfm_paths.cluster_path.str.split(';')
# Clean list
rfm_paths['path_list'] = rfm_paths.path_list.apply(lambda x: clear_path(x))
# Clean string
rfm_paths['cluster_path'] = rfm_paths.path_list.apply(lambda x: ':'.join(x))
# All paths larger than 1
col_paths = rfm_paths[rfm_paths.path_list.map(len)>1]['path_list']
# Get all edges
col_paths = col_paths.apply(get_edges)
# Set of edges
edges = set()
col_paths.apply(lambda x: [edges.add(y) for y in x])

# Get all clusters
paths = rfm_paths.path_list.to_list()
cohort_set = {x for l in paths for x in l}


# Node graph
RFM_graph = nx.MultiDiGraph()
# Add nodes
RFM_graph.add_nodes_from(cohort_set)
# Add edges
RFM_graph.add_edges_from(edges)


# Plot Graph
pos = nx.spring_layout(RFM_graph, k=0.7, iterations=20)
nx.draw_networkx_nodes(RFM_graph, pos, cmap=plt.get_cmap('jet'), node_size=400, node_color='white', edgecolors='black')
nx.draw_networkx_labels(RFM_graph, pos)
nx.draw_networkx_edges(RFM_graph, pos, edge_color='black', arrows=True)
plt.show()
