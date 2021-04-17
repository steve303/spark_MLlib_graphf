from pyspark.ml.classification import RandomForestClassifier
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
sc = SparkContext()
sqlContext = SQLContext(sc)


def predict(df_train, df_test):
    # TODO: Train random forest classifier

    # Hint: Column names in the given dataframes need to match the column names
    # expected by the random forest classifier `train` and `transform` functions.
    # Or you can alternatively specify which columns the `train` and `transform`
    # functions should use

    # Result: Result should be a list with the trained model's predictions
    # for all the test data points

    #organize train data:
    colnames = df_train.schema.names
    vecAssembler = VectorAssembler(inputCols=colnames[:-1], outputCol='features')
    df_trans_train = vecAssembler.transform(df_train)

    #organize test data:
    colnames_test = df_test.schema.names
    vecAssembler_test = VectorAssembler(inputCols=colnames_test, outputCol='features')
    df_trans_test = vecAssembler.transform(df_test)
    #df_trans_test.show()
    
    #set up classifier and predict:
    rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'label', numTrees= 500, seed=1, maxDepth=30)
    rfModel = rf.fit(df_trans_train)
    predictions = rfModel.transform(df_trans_test)
    #predictions.select('prediction', 'probability').show(10)
    #predictions.show()
    data = predictions.select(['prediction']).collect()
    data_list = [row['prediction'] for row in data]
    #print(data_list)
    return data_list

def parse_line(line):
    # TODO: Parse data from line into an RDD
    line = line.strip().split(',')
    line[0:] = [float(line[i]) for i in range(len(line))]
    return line

                            
def main():
    raw_training_data = sc.textFile("dataset/training.data")
    # TODO: Convert text file into an RDD which can be converted to a DataFrame
    # Hint: For types and format look at what the format required by the
    # `train` method for the random forest classifier
    # Hint 2: Look at the imports above
    rdd_train = raw_training_data.map(parse_line) 
    #print(rdd_train.take(1))
    #print(len(rdd_train.take(1)[0]))

    # TODO: Create dataframe from the RDD
    colnames = ['c_' + str(i) for i in range(len(rdd_train.take(1)[0]) - 1)] + ['label']
    df_train = rdd_train.toDF(colnames)
    #df_train.show()

    raw_test_data = sc.textFile("dataset/test-features.data")

    # TODO: Convert text file lines into an RDD we can use later
    rdd_test = raw_test_data.map(parse_line)

    # TODO:Create dataframe from RDD
    
    df_test = rdd_test.toDF(colnames[0:-1])
    #df_test.show()

    predictions = predict(df_train, df_test)

    # You can take a look at dataset/test-labels.data to see if your
    # predictions were right
    for pred in predictions:
        print(int(pred))


if __name__ == "__main__":
    main()
    
