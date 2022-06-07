from pycaret.regression import load_model, predict_model
import pandas as pd


def predicting(airflow_home, location_prediction, location_model, model_name,
               file_name_to_predict, file_name_predicted):
    """
    Evaluate the model using the training data. It also logs the metadata of the ML pipeline.
    :return:
    """
    df = _get_data_to_predict(airflow_home, location_prediction,
                              file_name_to_predict)
    model = _loading_model(airflow_home, location_model, model_name)
    _predicting_with_model(model, df, airflow_home, location_prediction,
                           file_name_predicted)


def _get_data_to_predict(airflow_home, location_prediction,
                         file_name_to_predict):
    """
    Get data from the csv file stored in previous step.
    :return:
    """
    df = pd.read_csv(airflow_home + location_prediction + file_name_to_predict)
    df['weight_per_length_of_train'] = round(
        df['train_weight'] / df['train_length'], 1)
    df['weight_per_wagon_of_train'] = round(
        df['train_weight'] / df['wagon_count'], 1)
    df.drop(columns=['train_weight', 'wagon_count'], axis=1, inplace=True)
    return df


def _loading_model(airflow_home, location_model, model_name):
    """
    Load the model from the specified location.
    :return model:
    """
    model_path = airflow_home + location_model + model_name
    model = load_model(model_path)
    return model


# Adding a custom metric
def _predicting_with_model(model, df, airflow_home, location_prediction,
                           file_name_predicted):
    """
    Predicting arrival_delay.
    :return:
    """
    predictions = predict_model(model, df)  # Using pycaret
    predictions.rename(columns={'Label': 'arrival_delay'}, inplace=True)
    predictions.to_csv(airflow_home + location_prediction +
                       file_name_predicted,
                       index=False)
