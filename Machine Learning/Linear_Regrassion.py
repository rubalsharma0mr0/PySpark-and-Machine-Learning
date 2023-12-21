from pyspark.ml.regression import LinearRegression

from spark_session_utils import SparkSessionUtils

if __name__ == '__main__':
    # Create a SparkSession
    print("Creating SparkSession...")
    spark_context = SparkSessionUtils('Linear Regression').get_spark_session()

    # Load the data using LIBSVM format
    print("Loading data...")
    '''
    LIBSVM implements the sequential minimal optimization (SMO) algorithm for kernelized support vector machines (SVMs),
    supporting classification and regression. LIBLINEAR implements linear SVMs and logistic regression models trained
    using a coordinate descent algorithm.
    '''
    df = spark_context.read.format('libsvm').load('../data_source/Linear_Regression/sample_linear_regression_data.txt')

    # Split the data into training and testing datasets
    print("Splitting data into training and testing datasets...")
    training_df, testing_df = df.randomSplit([0.7, 0.3])

    # Create a LinearRegression model
    print("Creating LinearRegression model...")
    linear_regression = LinearRegression(featuresCol='features', labelCol='label', predictionCol='prediction')

    # Fit the model to the training data
    print("Fitting the model to the training data...")
    linear_regression_model = linear_regression.fit(training_df)

    # Print the model coefficients and intercept
    print(f'Model Coefficients: {linear_regression_model.coefficients}')
    print(f'Intercept: {linear_regression_model.intercept}')

    # Get the summary statistics of the linear regression model
    print("Getting the summary statistics of the model...")
    linear_regression_summary = linear_regression_model.summary

    # Print the R-squared value and coefficient standard errors
    print(f'r2: {linear_regression_summary.r2}')
    print(f'coefficientStandardErrors: {linear_regression_summary.coefficientStandardErrors}')

    # Evaluate the model on the testing data
    print("Evaluating the model on the testing data...")
    test_result = linear_regression_model.evaluate(testing_df)

    # Show the residuals of the predictions
    print("Showing the residuals of the predictions...")
    test_result.residuals.show()

    # Print the R-squared value and root mean squared error of the test results
    print(f'test_result r2: {test_result.r2}')
    print(f'test_result rootMeanSquaredError: {test_result.rootMeanSquaredError}')

    # Model Deployment
    # Select the unlabeled data for prediction
    print("Selecting unlabeled data for prediction...")
    unlabeled_data = testing_df.select('features')

    # Make predictions on the unlabeled data
    print("Making predictions on the unlabeled data...")
    prediction = linear_regression_model.transform(unlabeled_data)

    # Show the predictions
    print("Showing the predictions...")
    prediction.show()
