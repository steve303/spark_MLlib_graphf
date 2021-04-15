from pyspark import *
from pyspark.sql import SparkSession
from graphframes import *

sc = SparkContext()
spark = SparkSession.builder.appName('fun').getOrCreate()


def get_connected_components(graphframe):
    # TODO:
    # get_connected_components is given a graphframe that represents the given graph
    # It returns a list of lists of ids.
    # For example, [[a_1, a_2, ...], [b_1, b_2, ...], ...]
    # then a_1, a_2, ..., a_n lie in the same component,
    # b_1, b2, ..., b_m lie in the same component, etc
    dict01 = {}
    df = graphframe.connectedComponents()
    data = df.collect()
    for val in data:
        if val[1] not in dict01:
            dict01[val[1]] = [val[0]]
        else:
            dict01[val[1]].append(val[0])
    #print(dict01)        
    results = []
    vertices = dict01.values()
    for v in vertices:
        results.append(v)
    return results

if __name__ == "__main__":
    vertex_list = []
    edge_list = []
    with open('dataset/graph.data') as f:  # Do not modify
        for line in f:
            data = line.strip().split(' ')
            src = data[0]  # TODO: Parse src from line
            dst_list = data[1:]  # TODO: Parse dst_list from line
            vertex_list.append((src,))
            edge_list += [(src, dst) for dst in dst_list]

    vertices = spark.createDataFrame(vertex_list, ['id'])  # TODO: Create vertices dataframe
    edges = spark.createDataFrame(edge_list, ['src', 'dst'])  # TODO: Create edges dataframe

    g = GraphFrame(vertices, edges)
    #print(g)
    #display(g.vertices)
    sc.setCheckpointDir("/tmp/connected-components")

    result = get_connected_components(g)
    for line in result:
        print(' '.join(line))
