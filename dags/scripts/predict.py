# -*- coding: utf-8 -*-
"""
This script is used to do prediction based on trained model

Usage:
    python ./scripts/predict.py

"""
from pathlib import Path
import click
import pandas as pd
from pycaret.classification import load_model, predict_model
from utility import parse_config, set_logger


@click.command()
@click.argument("config_file", type=str, default="scripts/config.yml")
def predict(config_file):
    """
    Main function that runs predictions

    Args:
        config_file [str]: path to config file

    Returns:
        None
    """
    ##################
    # configure logger
    ##################
    logger = set_logger("./script_logs/predict.log")

    ##################
    # Load config from config file
    ##################
    logger.info(f"Load config from {config_file}")
    config = parse_config(config_file)

    model_path = Path(config["predict"]["model_path"])
    processed_test = config["predict"]["processed_test"]
    predicted_file = config["predict"]["predicted_file"]
    export_result = config["predict"]["export_result"]

    logger.info(f"config: {config['predict']}")

    ##################
    # Load model & test set
    ##################
    # Load model
    logger.info(
        f"-------------------Load the trained model-------------------")
    model = load_model(model_path)

    # Load and prepare data
    logger.info(f"Load the data to predict {processed_test}")
    data = pd.read_csv(processed_test)
    data['weight_per_length_of_train'] = round(
        data['train_weight'] / data['train_length'], 1)
    data['weight_per_wagon_of_train'] = round(
        data['train_weight'] / data['wagon_count'], 1)
    data.drop(columns=['train_weight', 'wagon_count'], axis=1, inplace=True)

    ##################
    # Make prediction and export file
    ##################
    logger.info(f"-------------------Predict and evaluate-------------------")
    predictions = predict_model(model, data)  # Using pycaret
    predictions.rename(columns={'Label': 'arrival_delay'}, inplace=True)

    predictions.to_csv(predicted_file, index=False)


if __name__ == "__main__":
    predict()
