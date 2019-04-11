from pyspark.mllib.tree import RandomForest
from pyspark import SparkContext

sc = SparkContext()


def predict(training_data, test_data):
    # TODO: Train random forest classifier from given data
    # Result should be an RDD with the prediction of the random forest for each
    # test data point
    return None


if __name__ == "__main__":
    raw_training_data = sc.textFile("dataset/training.data")
    raw_test_data = sc.textFile("dataset/test-features.data")

    # TODO: Parse RDD from raw training data
    training_data = None

    # TODO: Parse RDD from raw test data
    test_data = None

    predictions = predict(training_data, test_data)

    # You can take a look at dataset/test-labels.data to see if your
    # predictions were right
    for pred in predictions.collect():
        print(int(pred))
