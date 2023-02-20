# -*- coding: utf-8 -*-
"""
This script is used to train and export ML model according to config
Usage:
    python ./scripts/train.py
"""
from pathlib import Path
import click
import pandas as pd
from pycaret.regression import *
from utility import parse_config, set_logger, calculate_rMSE


@click.command()
@click.argument("config_file", type=str, default="scripts/config.yml")
def train(config_file):
    """
    Main function that trains & persists model based on training set
    Args:
        config_file [str]: path to config file
    Returns:
        None
    """
    ##################
    # configure logger
    ##################
    logger = set_logger("./script_logs/train.log")

    ##################
    # Load config from config file
    ##################
    logger.info(f"Load config from {config_file}")
    config = parse_config(config_file)

    processed_train = Path(config["train"]["processed_train"])
    model_path = Path(config["train"]["model_path"])

    logger.info(f"config: {config['train']}")

    ##################
    # Load data
    ##################
    logger.info(
        f"-------------------Load the processed data-------------------")
    df = pd.read_csv(processed_train, low_memory=False)
    logger.info(f"shape: {df.shape}")

    ##################
    # Setup
    ##################
    # Setup Pycaret
    logger.info(f"-------------------Setup pycaret-------------------")

    regressor = setup(data=df,
                      target='arrival_delay',
                      session_id=42,
                      normalize=True,
                      combine_rare_levels=True,
                      rare_level_threshold=0.05,
                      use_gpu=True,
                      log_experiment=True,
                      experiment_name='cfl_reg',
                      silent=True)

    # Adding an additional metric
    add_metric('rMSE', 'relative_MSE', calculate_rMSE, greater_is_better=False)

    logger.info(f"-------------------End setup pycaret-------------------")

    ##################
    # train model
    ##################
    # Train model
    logger.info(f"-------------------Training model-------------------")
    lightgbm = create_model(estimator='lightgbm', verbose=False)

    # Finalizing model
    model = finalize_model(lightgbm)
    predictions = predict_model(model)
    logger.info(f"Metrics:\n {pull(model)}")

    ##################
    # Saving model
    ##################

    logger.info(f"-------------------Saving model-------------------")
    save_model(model, model_path)

    logger.info(f"-------------------End saving model-------------------")


if __name__ == "__main__":
    train()