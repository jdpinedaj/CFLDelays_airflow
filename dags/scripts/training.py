from pycaret.regression import *
import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error
import logging


def training_model(airflow_home, location_data, file_name, location_model, model_name):
    """
    Get data, build pipeline, train the model.
    :return:
    """
    df = _get_data(airflow_home, location_data, file_name)
    model = _build_ml_pipeline(df)
    _evaluate_model(model)
    _saving_model(model, airflow_home, location_model, model_name)


def _get_data(airflow_home, location_data, file_name):
    """
    Get data from the csv file stored in previous step.
    :return:
    """
    df = pd.read_csv(airflow_home + location_data + file_name)

    return df


def _build_ml_pipeline(df):
    """
    Build the ML pipeline using pycaret.
    :return Pipeline:
    """
    # Set up
    regressor = setup(
        data=df,
        target="arrival_delay",
        session_id=42,
        normalize=True,
        combine_rare_levels=True,
        rare_level_threshold=0.05,
        use_gpu=True,
        log_experiment=False,  # True for MLFlow
        experiment_name="cfl_reg",
        silent=True,
    )

    # Adding an additional metric
    add_metric("rMSE", "relative_MSE", _calculate_rMSE, greater_is_better=False)

    # Training model
    lightgbm = create_model(estimator="lightgbm", verbose=False)
    model = finalize_model(lightgbm)
    return model


def _evaluate_model(model):
    """
    Evaluate the model using the training data. It also logs the metadata of the ML pipeline.
    :return:
    """
    # Evaluate the model
    predictions = predict_model(model)
    logging.info("Predictions: " + str(predictions))
    logging.info(f"Metrics:\n {pull(model)}")


def _saving_model(model, airflow_home, location_model, model_name):
    """
    Save the model in the specified location.
    """
    model_path = airflow_home + location_model + model_name
    save_model(model, model_path)


# Adding a custom metric
def _calculate_rMSE(y, y_pred):
    """
    Calculating the rMSE in percentage.
    """
    mse = mean_squared_error(y_pred, y)
    rmse = np.sqrt(mse)
    return rmse / np.mean(y)
