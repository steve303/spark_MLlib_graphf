from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors
import pyspark.sql.functions as F

from pyspark.ml.feature import VectorAssembler
############################################
#### PLEASE USE THE GIVEN PARAMETERS     ###
#### FOR TRAINING YOUR KMEANS CLUSTERING ###
#### MODEL                               ###
############################################

NUM_CLUSTERS = 4
SEED = 0
MAX_ITERATIONS = 100
INITIALIZATION_MODE = "random"

sc = SparkContext()
sqlContext = SQLContext(sc)


def get_clusters(df, num_clusters, max_iterations, initialization_mode,
                 seed):
    # TODO:
    # Use the given data and the cluster pparameters to train a K-Means model
    # Find the cluster id corresponding to data point (a car)
    # Return a list of lists of the titles which belong to the same cluster
    # For example, if the output is [["Mercedes", "Audi"], ["Honda", "Hyundai"]]
    # Then "Mercedes" and "Audi" should have the same cluster id, and "Honda" and
    # "Hyundai" should have the same cluster id
    colnames = df.schema.names
    #print(colnames)
    vecAssembler = VectorAssembler(inputCols=colnames[1:], outputCol='features')
    df_kmeans = vecAssembler.transform(df)
    #df_kmeans.show()
    kmeans = KMeans().setK(num_clusters).setSeed(seed).setFeaturesCol('features').setMaxIter(max_iterations).setInitMode(initialization_mode)
    model = kmeans.fit(df_kmeans)
    transformed = model.transform(df_kmeans)
    #transformed.show()
    result_dict = {}
    #data = transformed.select(['id','prediction'])
    #for i in range(transformed.count()):
    data = transformed.collect()

    for val in data:
        if val['prediction'] not in result_dict:
            result_dict[val['prediction']] = [val['id']]
        else:
            result_dict[val['prediction']].append(val['id'])
    #print(result_dict)
    values = result_dict.values()
    result_list = []
    for val in values:
        result_list.append(val)
    return result_list


def parse_line(line):
    # TODO: Parse data from line into an RDD
    # Hint: Look at the data format and columns required by the KMeans fit and
    # transform functions
    line = line.strip().split(',')
    line[0] = line[0].strip('"')
    line[1:] = [float(line[i]) for i in range(1, len(line))]
    return line


if __name__ == "__main__":
    f = sc.textFile("dataset/cars.data")
    '''
    data = f.collect()
    for val in data:
        print(val[0:], type(val), len(val))
        var = val.strip().split(",")
        print(var)
        print(len(var))
    '''
    rdd = f.map(parse_line)
    # TODO: Convert RDD into a dataframe
    colnames = ['id'] + ['f_' + str(x) for x in range(11)]
    df = rdd.toDF(colnames)
    #print(rdd.collect())
    #df.show()

    clusters = get_clusters(df, NUM_CLUSTERS, MAX_ITERATIONS,
                            INITIALIZATION_MODE, SEED)
    for cluster in clusters:
        print(','.join(cluster))
