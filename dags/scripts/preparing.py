# -*- coding: utf-8 -*-
"""
This script is used to run convert the raw data of train_data_final to train data for applying ml
It is designed to be idempotent [stateless transformation]
Usage:
    python ./scripts/preparing.py
"""

import pandas as pd
import numpy as np
import datetime as dt
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)


def preparing():
    """
    Preparing function that load raw data of train_data_final and convert to train data for applying ml
    Args:
        config_file [str]: path to config file
    Returns:
        None
    """

    ##################
    # Load paths
    ##################

    train_data_file = "../data/train_data_final.xlsx"
    incident_concerne_file = "../data/incident_concerne.xlsx"
    incidents_file = "../data/incidents.xlsx"
    df_final_file = "../data/df_final_for_ml.csv"
    routes_file = "../data/routes.csv"

    ##################
    # Creating routes
    ##################
    df = pd.read_excel(train_data_file)

    station_columns = [col for col in df.columns if 'Station_Name' in col]
    routes_data = df[station_columns]
    routes_data = routes_data.drop_duplicates(keep='first').reset_index(
        drop=True)
    routes_data = routes_data.replace({np.nan: 0})

    routes = pd.DataFrame()

    routes['stations'] = [
        list(x) for x in zip(*[routes_data[col] for col in routes_data])
    ]
    routes['stations'] = routes['stations'].apply(
        lambda x: list(dict.fromkeys(x)))
    for index, row in routes.iterrows():
        row['stations'] = [x for x in row['stations'] if x != 0]

    ##################
    # Data transformation
    ##################
    # traindata
    traindata = pd.read_excel(train_data_file)

    # incident_concerne
    incident_concerne = pd.read_excel(incident_concerne_file)
    incident_concerne.columns = incident_concerne.columns.str.strip(
    ).str.lower()
    incident_concerne = incident_concerne[[
        'idincident', 'idtrain_lot', 'annule_trainlot'
    ]]
    incident_concerne = incident_concerne.drop_duplicates(subset='idincident',
                                                          keep='first')
    incident_concerne = incident_concerne[incident_concerne['annule_trainlot']
                                          == 1]
    incident_concerne = incident_concerne.drop(columns=['annule_trainlot'])

    # incidents
    incidents = pd.read_excel(incidents_file)
    incidents.columns = incidents.columns.str.strip().str.lower()
    incidents = incidents[[
        'idincident', 'idgare', 'type_incident', 'dateh_incident', 'lieu',
        'statut', 'statut_commercial', 'statut_financier', 'gravite',
        'motif_client', 'commentaire'
    ]]

    # Merging incident data
    df_incidents = incident_concerne.merge(incidents,
                                           on=['idincident'],
                                           how='left')

    # Remove rows that do not have data either in idtrain_lot and in idgare, because it is not possible to merge with train_data_final
    #df_incidents = df_incidents.dropna(subset=['idtrain_lot', 'idgare'], how='all') # I will use this if I find a way to consider also idgare, and not the next two lines
    df_incidents = df_incidents.dropna(subset=['idtrain_lot'])
    df_incidents = df_incidents.drop(columns=['idgare'])

    # Merging train_data final with incidents using idtrain_lot
    train_data = traindata.merge(df_incidents,
                                 left_on=['IDTRAIN_LOT'],
                                 right_on=['idtrain_lot'],
                                 how='left')
    train_data = train_data.drop(columns=['idtrain_lot'])

    # train_tare_weight
    cols_to_sum_tare = [
        'Tare_Weight_0', 'Tare_Weight_1', 'Tare_Weight_2', 'Tare_Weight_3',
        'Tare_Weight_4', 'Tare_Weight_5', 'Tare_Weight_6', 'Tare_Weight_7',
        'Tare_Weight_8', 'Tare_Weight_9', 'Tare_Weight_10', 'Tare_Weight_11',
        'Tare_Weight_12', 'Tare_Weight_13', 'Tare_Weight_14', 'Tare_Weight_15',
        'Tare_Weight_16', 'Tare_Weight_17', 'Tare_Weight_18', 'Tare_Weight_19',
        'Tare_Weight_20', 'Tare_Weight_21', 'Tare_Weight_22', 'Tare_Weight_23',
        'Tare_Weight_24', 'Tare_Weight_25', 'Tare_Weight_26', 'Tare_Weight_27',
        'Tare_Weight_28', 'Tare_Weight_29', 'Tare_Weight_30', 'Tare_Weight_31',
        'Tare_Weight_32', 'Tare_Weight_33', 'Tare_Weight_34', 'Tare_Weight_35'
    ]

    train_data['train_tare_weight'] = train_data[cols_to_sum_tare].sum(axis=1)

    ## Dividing set in two

    data = train_data.copy()
    data['IDTRAIN'] = data['IDTRAIN'].astype(str)

    # Data related to stations
    # TODO: Fix this hard-code
    station_data = data.columns[65:305]
    # Creating a general dataframe with common attributes
    dataset_1 = data.copy()
    dataset_1 = dataset_1.drop(station_data, axis=1)  #1 in the whiteboard
    #weather data columns
    dataset_1.rename(columns={
        'precipIntensity': 'precipIntensity_final_station',
        'precipProbability': 'precipProbability_final_station',
        'temperature': 'temperature_final_station',
        'apparentTemperature': 'apparentTemperature_final_station',
        'dewPoint': 'dewPoint_final_station',
        'humidity': 'humidity_final_station',
        'windSpeed': 'windSpeed_final_station',
        'windBearing': 'windBearing_final_station',
        'cloudCover': 'cloudCover_final_station',
        'uvIndex': 'uvIndex_final_station',
        'visibility': 'visibility_final_station'
    },
                     inplace=True)
    dataset_1.drop(columns=[
        'IDTRAIN_LOT', 'IDTRAIN_JALON_origin', 'IDGARE_origin', 'Code_origin',
        'IDGARE_Parent_origin', 'IDTRAIN_JALON_destination',
        'IDGARE_destination', 'Code_destination', 'IDGARE_Parent_destination',
        'IDTRAIN_Parent', 'IDTRAIN_ETAPE'
    ],
                   axis=1,
                   inplace=True)

    # Creating a new dataframe WITHOUT the common features
    dataset_2 = data.copy()
    dataset_2 = dataset_2[station_data]  #2 in the whiteboard
    dataset_2.insert(0, 'IDTRAIN', data['IDTRAIN'])

    # Changing the numeration and names of stations data
    # TODO: Fix this hard-code
    dataset2_fields = [
        'IDTRAIN', 'Station_Name_0', 'Station_Name_1', 'Station_Name_2',
        'Station_Name_3', 'Station_Name_4', 'Station_Name_5', 'Station_Name_6',
        'Station_Name_7', 'Station_Name_8', 'Station_Name_9',
        'Station_Name_10', 'Station_Name_11', 'Station_Name_12',
        'Station_Name_13', 'Station_Name_14', 'Station_Name_15',
        'Station_Name_16', 'Station_Name_17', 'Station_Name_18',
        'Station_Name_19', 'Station_Name_20', 'Station_Name_21',
        'Station_Name_22', 'Station_Name_23', 'Plan_Timestamp_0',
        'Plan_Timestamp_1', 'Plan_Timestamp_2', 'Plan_Timestamp_3',
        'Plan_Timestamp_4', 'Plan_Timestamp_5', 'Plan_Timestamp_6',
        'Plan_Timestamp_7', 'Plan_Timestamp_8', 'Plan_Timestamp_9',
        'Plan_Timestamp_10', 'Plan_Timestamp_11', 'Plan_Timestamp_12',
        'Plan_Timestamp_13', 'Plan_Timestamp_14', 'Plan_Timestamp_15',
        'Plan_Timestamp_16', 'Plan_Timestamp_17', 'Plan_Timestamp_18',
        'Plan_Timestamp_19', 'Plan_Timestamp_20', 'Plan_Timestamp_21',
        'Plan_Timestamp_22', 'Plan_Timestamp_23', 'Actual_Timestamp_0',
        'Actual_Timestamp_1', 'Actual_Timestamp_2', 'Actual_Timestamp_3',
        'Actual_Timestamp_4', 'Actual_Timestamp_5', 'Actual_Timestamp_6',
        'Actual_Timestamp_7', 'Actual_Timestamp_8', 'Actual_Timestamp_9',
        'Actual_Timestamp_10', 'Actual_Timestamp_11', 'Actual_Timestamp_12',
        'Actual_Timestamp_13', 'Actual_Timestamp_14', 'Actual_Timestamp_15',
        'Actual_Timestamp_16', 'Actual_Timestamp_17', 'Actual_Timestamp_18',
        'Actual_Timestamp_19', 'Actual_Timestamp_20', 'Actual_Timestamp_21',
        'Actual_Timestamp_22', 'Actual_Timestamp_23',
        'Time_From_Prior_Plan_Mins_0', 'Time_From_Prior_Plan_Mins_1',
        'Time_From_Prior_Plan_Mins_2', 'Time_From_Prior_Plan_Mins_3',
        'Time_From_Prior_Plan_Mins_4', 'Time_From_Prior_Plan_Mins_5',
        'Time_From_Prior_Plan_Mins_6', 'Time_From_Prior_Plan_Mins_7',
        'Time_From_Prior_Plan_Mins_8', 'Time_From_Prior_Plan_Mins_9',
        'Time_From_Prior_Plan_Mins_10', 'Time_From_Prior_Plan_Mins_11',
        'Time_From_Prior_Plan_Mins_12', 'Time_From_Prior_Plan_Mins_13',
        'Time_From_Prior_Plan_Mins_14', 'Time_From_Prior_Plan_Mins_15',
        'Time_From_Prior_Plan_Mins_16', 'Time_From_Prior_Plan_Mins_17',
        'Time_From_Prior_Plan_Mins_18', 'Time_From_Prior_Plan_Mins_19',
        'Time_From_Prior_Plan_Mins_20', 'Time_From_Prior_Plan_Mins_21',
        'Time_From_Prior_Plan_Mins_22', 'Time_From_Prior_Plan_Mins_23',
        'Depart_Variance_Mins_0', 'Depart_Variance_Mins_1',
        'Depart_Variance_Mins_2', 'Depart_Variance_Mins_3',
        'Depart_Variance_Mins_4', 'Depart_Variance_Mins_5',
        'Depart_Variance_Mins_6', 'Depart_Variance_Mins_7',
        'Depart_Variance_Mins_8', 'Depart_Variance_Mins_9',
        'Depart_Variance_Mins_10', 'Depart_Variance_Mins_11',
        'Depart_Variance_Mins_12', 'Depart_Variance_Mins_13',
        'Depart_Variance_Mins_14', 'Depart_Variance_Mins_15',
        'Depart_Variance_Mins_16', 'Depart_Variance_Mins_17',
        'Depart_Variance_Mins_18', 'Depart_Variance_Mins_19',
        'Depart_Variance_Mins_20', 'Depart_Variance_Mins_21',
        'Depart_Variance_Mins_22', 'Depart_Variance_Mins_23',
        'Arrive_Variance_Mins_0', 'Arrive_Variance_Mins_1',
        'Arrive_Variance_Mins_2', 'Arrive_Variance_Mins_3',
        'Arrive_Variance_Mins_4', 'Arrive_Variance_Mins_5',
        'Arrive_Variance_Mins_6', 'Arrive_Variance_Mins_7',
        'Arrive_Variance_Mins_8', 'Arrive_Variance_Mins_9',
        'Arrive_Variance_Mins_10', 'Arrive_Variance_Mins_11',
        'Arrive_Variance_Mins_12', 'Arrive_Variance_Mins_13',
        'Arrive_Variance_Mins_14', 'Arrive_Variance_Mins_15',
        'Arrive_Variance_Mins_16', 'Arrive_Variance_Mins_17',
        'Arrive_Variance_Mins_18', 'Arrive_Variance_Mins_19',
        'Arrive_Variance_Mins_20', 'Arrive_Variance_Mins_21',
        'Arrive_Variance_Mins_22', 'Arrive_Variance_Mins_23',
        'Travel_Time_Mins_0', 'Travel_Time_Mins_1', 'Travel_Time_Mins_2',
        'Travel_Time_Mins_3', 'Travel_Time_Mins_4', 'Travel_Time_Mins_5',
        'Travel_Time_Mins_6', 'Travel_Time_Mins_7', 'Travel_Time_Mins_8',
        'Travel_Time_Mins_9', 'Travel_Time_Mins_10', 'Travel_Time_Mins_11',
        'Travel_Time_Mins_12', 'Travel_Time_Mins_13', 'Travel_Time_Mins_14',
        'Travel_Time_Mins_15', 'Travel_Time_Mins_16', 'Travel_Time_Mins_17',
        'Travel_Time_Mins_18', 'Travel_Time_Mins_19', 'Travel_Time_Mins_20',
        'Travel_Time_Mins_21', 'Travel_Time_Mins_22', 'Travel_Time_Mins_23',
        'Idle_Time_Mins_0', 'Idle_Time_Mins_1', 'Idle_Time_Mins_2',
        'Idle_Time_Mins_3', 'Idle_Time_Mins_4', 'Idle_Time_Mins_5',
        'Idle_Time_Mins_6', 'Idle_Time_Mins_7', 'Idle_Time_Mins_8',
        'Idle_Time_Mins_9', 'Idle_Time_Mins_10', 'Idle_Time_Mins_11',
        'Idle_Time_Mins_12', 'Idle_Time_Mins_13', 'Idle_Time_Mins_14',
        'Idle_Time_Mins_15', 'Idle_Time_Mins_16', 'Idle_Time_Mins_17',
        'Idle_Time_Mins_18', 'Idle_Time_Mins_19', 'Idle_Time_Mins_20',
        'Idle_Time_Mins_21', 'Idle_Time_Mins_22', 'Idle_Time_Mins_23',
        'KM_Distance_Event_0', 'KM_Distance_Event_1', 'KM_Distance_Event_2',
        'KM_Distance_Event_3', 'KM_Distance_Event_4', 'KM_Distance_Event_5',
        'KM_Distance_Event_6', 'KM_Distance_Event_7', 'KM_Distance_Event_8',
        'KM_Distance_Event_9', 'KM_Distance_Event_10', 'KM_Distance_Event_11',
        'KM_Distance_Event_12', 'KM_Distance_Event_13', 'KM_Distance_Event_14',
        'KM_Distance_Event_15', 'KM_Distance_Event_16', 'KM_Distance_Event_17',
        'KM_Distance_Event_18', 'KM_Distance_Event_19', 'KM_Distance_Event_20',
        'KM_Distance_Event_21', 'KM_Distance_Event_22', 'KM_Distance_Event_23',
        'KM_HR_Event_0', 'KM_HR_Event_1', 'KM_HR_Event_2', 'KM_HR_Event_3',
        'KM_HR_Event_4', 'KM_HR_Event_5', 'KM_HR_Event_6', 'KM_HR_Event_7',
        'KM_HR_Event_8', 'KM_HR_Event_9', 'KM_HR_Event_10', 'KM_HR_Event_11',
        'KM_HR_Event_12', 'KM_HR_Event_13', 'KM_HR_Event_14', 'KM_HR_Event_15',
        'KM_HR_Event_16', 'KM_HR_Event_17', 'KM_HR_Event_18', 'KM_HR_Event_19',
        'KM_HR_Event_20', 'KM_HR_Event_21', 'KM_HR_Event_22', 'KM_HR_Event_23'
    ]

    dataset2_rename = [
        'IDTRAIN', 'Station_Name_1_dep', 'Station_Name_1_arriv',
        'Station_Name_2_dep', 'Station_Name_2_arriv', 'Station_Name_3_dep',
        'Station_Name_3_arriv', 'Station_Name_4_dep', 'Station_Name_4_arriv',
        'Station_Name_5_dep', 'Station_Name_5_arriv', 'Station_Name_6_dep',
        'Station_Name_6_arriv', 'Station_Name_7_dep', 'Station_Name_7_arriv',
        'Station_Name_8_dep', 'Station_Name_8_arriv', 'Station_Name_9_dep',
        'Station_Name_9_arriv', 'Station_Name_10_dep', 'Station_Name_10_arriv',
        'Station_Name_11_dep', 'Station_Name_11_arriv', 'Station_Name_12_dep',
        'Station_Name_12_arriv', 'Plan_Timestamp_1_dep',
        'Plan_Timestamp_1_arriv', 'Plan_Timestamp_2_dep',
        'Plan_Timestamp_2_arriv', 'Plan_Timestamp_3_dep',
        'Plan_Timestamp_3_arriv', 'Plan_Timestamp_4_dep',
        'Plan_Timestamp_4_arriv', 'Plan_Timestamp_5_dep',
        'Plan_Timestamp_5_arriv', 'Plan_Timestamp_6_dep',
        'Plan_Timestamp_6_arriv', 'Plan_Timestamp_7_dep',
        'Plan_Timestamp_7_arriv', 'Plan_Timestamp_8_dep',
        'Plan_Timestamp_8_arriv', 'Plan_Timestamp_9_dep',
        'Plan_Timestamp_9_arriv', 'Plan_Timestamp_10_dep',
        'Plan_Timestamp_10_arriv', 'Plan_Timestamp_11_dep',
        'Plan_Timestamp_11_arriv', 'Plan_Timestamp_12_dep',
        'Plan_Timestamp_12_arriv', 'Actual_Timestamp_1_dep',
        'Actual_Timestamp_1_arriv', 'Actual_Timestamp_2_dep',
        'Actual_Timestamp_2_arriv', 'Actual_Timestamp_3_dep',
        'Actual_Timestamp_3_arriv', 'Actual_Timestamp_4_dep',
        'Actual_Timestamp_4_arriv', 'Actual_Timestamp_5_dep',
        'Actual_Timestamp_5_arriv', 'Actual_Timestamp_6_dep',
        'Actual_Timestamp_6_arriv', 'Actual_Timestamp_7_dep',
        'Actual_Timestamp_7_arriv', 'Actual_Timestamp_8_dep',
        'Actual_Timestamp_8_arriv', 'Actual_Timestamp_9_dep',
        'Actual_Timestamp_9_arriv', 'Actual_Timestamp_10_dep',
        'Actual_Timestamp_10_arriv', 'Actual_Timestamp_11_dep',
        'Actual_Timestamp_11_arriv', 'Actual_Timestamp_12_dep',
        'Actual_Timestamp_12_arriv', 'Time_From_Prior_Plan_Mins_1_dep',
        'Time_From_Prior_Plan_Mins_1_arriv', 'Time_From_Prior_Plan_Mins_2_dep',
        'Time_From_Prior_Plan_Mins_2_arriv', 'Time_From_Prior_Plan_Mins_3_dep',
        'Time_From_Prior_Plan_Mins_3_arriv', 'Time_From_Prior_Plan_Mins_4_dep',
        'Time_From_Prior_Plan_Mins_4_arriv', 'Time_From_Prior_Plan_Mins_5_dep',
        'Time_From_Prior_Plan_Mins_5_arriv', 'Time_From_Prior_Plan_Mins_6_dep',
        'Time_From_Prior_Plan_Mins_6_arriv', 'Time_From_Prior_Plan_Mins_7_dep',
        'Time_From_Prior_Plan_Mins_7_arriv', 'Time_From_Prior_Plan_Mins_8_dep',
        'Time_From_Prior_Plan_Mins_8_arriv', 'Time_From_Prior_Plan_Mins_9_dep',
        'Time_From_Prior_Plan_Mins_9_arriv',
        'Time_From_Prior_Plan_Mins_10_dep',
        'Time_From_Prior_Plan_Mins_10_arriv',
        'Time_From_Prior_Plan_Mins_11_dep',
        'Time_From_Prior_Plan_Mins_11_arriv',
        'Time_From_Prior_Plan_Mins_12_dep',
        'Time_From_Prior_Plan_Mins_12_arriv', 'Depart_Variance_Mins_1_dep',
        'Depart_Variance_Mins_1_arriv', 'Depart_Variance_Mins_2_dep',
        'Depart_Variance_Mins_2_arriv', 'Depart_Variance_Mins_3_dep',
        'Depart_Variance_Mins_3_arriv', 'Depart_Variance_Mins_4_dep',
        'Depart_Variance_Mins_4_arriv', 'Depart_Variance_Mins_5_dep',
        'Depart_Variance_Mins_5_arriv', 'Depart_Variance_Mins_6_dep',
        'Depart_Variance_Mins_6_arriv', 'Depart_Variance_Mins_7_dep',
        'Depart_Variance_Mins_7_arriv', 'Depart_Variance_Mins_8_dep',
        'Depart_Variance_Mins_8_arriv', 'Depart_Variance_Mins_9_dep',
        'Depart_Variance_Mins_9_arriv', 'Depart_Variance_Mins_10_dep',
        'Depart_Variance_Mins_10_arriv', 'Depart_Variance_Mins_11_dep',
        'Depart_Variance_Mins_11_arriv', 'Depart_Variance_Mins_12_dep',
        'Depart_Variance_Mins_12_arriv', 'Arrive_Variance_Mins_1_dep',
        'Arrive_Variance_Mins_1_arriv', 'Arrive_Variance_Mins_2_dep',
        'Arrive_Variance_Mins_2_arriv', 'Arrive_Variance_Mins_3_dep',
        'Arrive_Variance_Mins_3_arriv', 'Arrive_Variance_Mins_4_dep',
        'Arrive_Variance_Mins_4_arriv', 'Arrive_Variance_Mins_5_dep',
        'Arrive_Variance_Mins_5_arriv', 'Arrive_Variance_Mins_6_dep',
        'Arrive_Variance_Mins_6_arriv', 'Arrive_Variance_Mins_7_dep',
        'Arrive_Variance_Mins_7_arriv', 'Arrive_Variance_Mins_8_dep',
        'Arrive_Variance_Mins_8_arriv', 'Arrive_Variance_Mins_9_dep',
        'Arrive_Variance_Mins_9_arriv', 'Arrive_Variance_Mins_10_dep',
        'Arrive_Variance_Mins_10_arriv', 'Arrive_Variance_Mins_11_dep',
        'Arrive_Variance_Mins_11_arriv', 'Arrive_Variance_Mins_12_dep',
        'Arrive_Variance_Mins_12_arriv', 'Travel_Time_Mins_1_dep',
        'Travel_Time_Mins_1_arriv', 'Travel_Time_Mins_2_dep',
        'Travel_Time_Mins_2_arriv', 'Travel_Time_Mins_3_dep',
        'Travel_Time_Mins_3_arriv', 'Travel_Time_Mins_4_dep',
        'Travel_Time_Mins_4_arriv', 'Travel_Time_Mins_5_dep',
        'Travel_Time_Mins_5_arriv', 'Travel_Time_Mins_6_dep',
        'Travel_Time_Mins_6_arriv', 'Travel_Time_Mins_7_dep',
        'Travel_Time_Mins_7_arriv', 'Travel_Time_Mins_8_dep',
        'Travel_Time_Mins_8_arriv', 'Travel_Time_Mins_9_dep',
        'Travel_Time_Mins_9_arriv', 'Travel_Time_Mins_10_dep',
        'Travel_Time_Mins_10_arriv', 'Travel_Time_Mins_11_dep',
        'Travel_Time_Mins_11_arriv', 'Travel_Time_Mins_12_dep',
        'Travel_Time_Mins_12_arriv', 'Idle_Time_Mins_1_dep',
        'Idle_Time_Mins_1_arriv', 'Idle_Time_Mins_2_dep',
        'Idle_Time_Mins_2_arriv', 'Idle_Time_Mins_3_dep',
        'Idle_Time_Mins_3_arriv', 'Idle_Time_Mins_4_dep',
        'Idle_Time_Mins_4_arriv', 'Idle_Time_Mins_5_dep',
        'Idle_Time_Mins_5_arriv', 'Idle_Time_Mins_6_dep',
        'Idle_Time_Mins_6_arriv', 'Idle_Time_Mins_7_dep',
        'Idle_Time_Mins_7_arriv', 'Idle_Time_Mins_8_dep',
        'Idle_Time_Mins_8_arriv', 'Idle_Time_Mins_9_dep',
        'Idle_Time_Mins_9_arriv', 'Idle_Time_Mins_10_dep',
        'Idle_Time_Mins_10_arriv', 'Idle_Time_Mins_11_dep',
        'Idle_Time_Mins_11_arriv', 'Idle_Time_Mins_12_dep',
        'Idle_Time_Mins_12_arriv', 'KM_Distance_Event_1_dep',
        'KM_Distance_Event_1_arriv', 'KM_Distance_Event_2_dep',
        'KM_Distance_Event_2_arriv', 'KM_Distance_Event_3_dep',
        'KM_Distance_Event_3_arriv', 'KM_Distance_Event_4_dep',
        'KM_Distance_Event_4_arriv', 'KM_Distance_Event_5_dep',
        'KM_Distance_Event_5_arriv', 'KM_Distance_Event_6_dep',
        'KM_Distance_Event_6_arriv', 'KM_Distance_Event_7_dep',
        'KM_Distance_Event_7_arriv', 'KM_Distance_Event_8_dep',
        'KM_Distance_Event_8_arriv', 'KM_Distance_Event_9_dep',
        'KM_Distance_Event_9_arriv', 'KM_Distance_Event_10_dep',
        'KM_Distance_Event_10_arriv', 'KM_Distance_Event_11_dep',
        'KM_Distance_Event_11_arriv', 'KM_Distance_Event_12_dep',
        'KM_Distance_Event_12_arriv', 'KM_HR_Event_1_dep',
        'KM_HR_Event_1_arriv', 'KM_HR_Event_2_dep', 'KM_HR_Event_2_arriv',
        'KM_HR_Event_3_dep', 'KM_HR_Event_3_arriv', 'KM_HR_Event_4_dep',
        'KM_HR_Event_4_arriv', 'KM_HR_Event_5_dep', 'KM_HR_Event_5_arriv',
        'KM_HR_Event_6_dep', 'KM_HR_Event_6_arriv', 'KM_HR_Event_7_dep',
        'KM_HR_Event_7_arriv', 'KM_HR_Event_8_dep', 'KM_HR_Event_8_arriv',
        'KM_HR_Event_9_dep', 'KM_HR_Event_9_arriv', 'KM_HR_Event_10_dep',
        'KM_HR_Event_10_arriv', 'KM_HR_Event_11_dep', 'KM_HR_Event_11_arriv',
        'KM_HR_Event_12_dep', 'KM_HR_Event_12_arriv'
    ]

    dataset2 = dataset_2.copy()
    dataset2 = dataset2[dataset2_fields]
    dataset2.columns = dataset2_rename

    logger.info("Numeration and names of stations data changed.")

    ## TODO: Optimise this cell because it is too manual...

    dataset_2 = dataset2.copy()

    # data_station_1
    data_station_1 = dataset_2.copy()
    data_station_1 = data_station_1[[
        'IDTRAIN', 'Station_Name_1_dep', 'Station_Name_1_arriv',
        'Plan_Timestamp_1_dep', 'Plan_Timestamp_1_arriv',
        'Actual_Timestamp_1_dep', 'Actual_Timestamp_1_arriv',
        'Time_From_Prior_Plan_Mins_1_dep', 'Time_From_Prior_Plan_Mins_1_arriv',
        'Depart_Variance_Mins_1_dep', 'Depart_Variance_Mins_1_arriv',
        'Arrive_Variance_Mins_1_dep', 'Arrive_Variance_Mins_1_arriv',
        'Travel_Time_Mins_1_dep', 'Travel_Time_Mins_1_arriv',
        'Idle_Time_Mins_1_dep', 'Idle_Time_Mins_1_arriv',
        'KM_Distance_Event_1_dep', 'KM_Distance_Event_1_arriv',
        'KM_HR_Event_1_dep', 'KM_HR_Event_1_arriv'
    ]]
    data_station_1.rename(columns={
        'Station_Name_1_dep': 'Station_Name_dep',
        'Station_Name_1_arriv': 'Station_Name_arriv',
        'Plan_Timestamp_1_dep': 'Plan_Timestamp_dep',
        'Plan_Timestamp_1_arriv': 'Plan_Timestamp_arriv',
        'Actual_Timestamp_1_dep': 'Actual_Timestamp_dep',
        'Actual_Timestamp_1_arriv': 'Actual_Timestamp_arriv',
        'Time_From_Prior_Plan_Mins_1_dep': 'Time_From_Prior_Plan_Mins_dep',
        'Time_From_Prior_Plan_Mins_1_arriv': 'Time_From_Prior_Plan_Mins_arriv',
        'Depart_Variance_Mins_1_dep': 'Depart_Variance_Mins_dep',
        'Depart_Variance_Mins_1_arriv': 'Depart_Variance_Mins_arriv',
        'Arrive_Variance_Mins_1_dep': 'Arrive_Variance_Mins_dep',
        'Arrive_Variance_Mins_1_arriv': 'Arrive_Variance_Mins_arriv',
        'Travel_Time_Mins_1_dep': 'Travel_Time_Mins_dep',
        'Travel_Time_Mins_1_arriv': 'Travel_Time_Mins_arriv',
        'Idle_Time_Mins_1_dep': 'Idle_Time_Mins_dep',
        'Idle_Time_Mins_1_arriv': 'Idle_Time_Mins_arriv',
        'KM_Distance_Event_1_dep': 'KM_Distance_Event_dep',
        'KM_Distance_Event_1_arriv': 'KM_Distance_Event_arriv',
        'KM_HR_Event_1_dep': 'KM_HR_Event_dep',
        'KM_HR_Event_1_arriv': 'KM_HR_Event_arriv'
    },
                          inplace=True)

    # data_station_2
    data_station_2 = dataset_2.copy()
    data_station_2 = data_station_2[[
        'IDTRAIN', 'Station_Name_2_dep', 'Station_Name_2_arriv',
        'Plan_Timestamp_2_dep', 'Plan_Timestamp_2_arriv',
        'Actual_Timestamp_2_dep', 'Actual_Timestamp_2_arriv',
        'Time_From_Prior_Plan_Mins_2_dep', 'Time_From_Prior_Plan_Mins_2_arriv',
        'Depart_Variance_Mins_2_dep', 'Depart_Variance_Mins_2_arriv',
        'Arrive_Variance_Mins_2_dep', 'Arrive_Variance_Mins_2_arriv',
        'Travel_Time_Mins_2_dep', 'Travel_Time_Mins_2_arriv',
        'Idle_Time_Mins_2_dep', 'Idle_Time_Mins_2_arriv',
        'KM_Distance_Event_2_dep', 'KM_Distance_Event_2_arriv',
        'KM_HR_Event_2_dep', 'KM_HR_Event_2_arriv'
    ]]
    data_station_2.rename(columns={
        'Station_Name_2_dep': 'Station_Name_dep',
        'Station_Name_2_arriv': 'Station_Name_arriv',
        'Plan_Timestamp_2_dep': 'Plan_Timestamp_dep',
        'Plan_Timestamp_2_arriv': 'Plan_Timestamp_arriv',
        'Actual_Timestamp_2_dep': 'Actual_Timestamp_dep',
        'Actual_Timestamp_2_arriv': 'Actual_Timestamp_arriv',
        'Time_From_Prior_Plan_Mins_2_dep': 'Time_From_Prior_Plan_Mins_dep',
        'Time_From_Prior_Plan_Mins_2_arriv': 'Time_From_Prior_Plan_Mins_arriv',
        'Depart_Variance_Mins_2_dep': 'Depart_Variance_Mins_dep',
        'Depart_Variance_Mins_2_arriv': 'Depart_Variance_Mins_arriv',
        'Arrive_Variance_Mins_2_dep': 'Arrive_Variance_Mins_dep',
        'Arrive_Variance_Mins_2_arriv': 'Arrive_Variance_Mins_arriv',
        'Travel_Time_Mins_2_dep': 'Travel_Time_Mins_dep',
        'Travel_Time_Mins_2_arriv': 'Travel_Time_Mins_arriv',
        'Idle_Time_Mins_2_dep': 'Idle_Time_Mins_dep',
        'Idle_Time_Mins_2_arriv': 'Idle_Time_Mins_arriv',
        'KM_Distance_Event_2_dep': 'KM_Distance_Event_dep',
        'KM_Distance_Event_2_arriv': 'KM_Distance_Event_arriv',
        'KM_HR_Event_2_dep': 'KM_HR_Event_dep',
        'KM_HR_Event_2_arriv': 'KM_HR_Event_arriv'
    },
                          inplace=True)

    # data_station_3
    data_station_3 = dataset_2.copy()
    data_station_3 = data_station_3[[
        'IDTRAIN', 'Station_Name_3_dep', 'Station_Name_3_arriv',
        'Plan_Timestamp_3_dep', 'Plan_Timestamp_3_arriv',
        'Actual_Timestamp_3_dep', 'Actual_Timestamp_3_arriv',
        'Time_From_Prior_Plan_Mins_3_dep', 'Time_From_Prior_Plan_Mins_3_arriv',
        'Depart_Variance_Mins_3_dep', 'Depart_Variance_Mins_3_arriv',
        'Arrive_Variance_Mins_3_dep', 'Arrive_Variance_Mins_3_arriv',
        'Travel_Time_Mins_3_dep', 'Travel_Time_Mins_3_arriv',
        'Idle_Time_Mins_3_dep', 'Idle_Time_Mins_3_arriv',
        'KM_Distance_Event_3_dep', 'KM_Distance_Event_3_arriv',
        'KM_HR_Event_3_dep', 'KM_HR_Event_3_arriv'
    ]]
    data_station_3.rename(columns={
        'Station_Name_3_dep': 'Station_Name_dep',
        'Station_Name_3_arriv': 'Station_Name_arriv',
        'Plan_Timestamp_3_dep': 'Plan_Timestamp_dep',
        'Plan_Timestamp_3_arriv': 'Plan_Timestamp_arriv',
        'Actual_Timestamp_3_dep': 'Actual_Timestamp_dep',
        'Actual_Timestamp_3_arriv': 'Actual_Timestamp_arriv',
        'Time_From_Prior_Plan_Mins_3_dep': 'Time_From_Prior_Plan_Mins_dep',
        'Time_From_Prior_Plan_Mins_3_arriv': 'Time_From_Prior_Plan_Mins_arriv',
        'Depart_Variance_Mins_3_dep': 'Depart_Variance_Mins_dep',
        'Depart_Variance_Mins_3_arriv': 'Depart_Variance_Mins_arriv',
        'Arrive_Variance_Mins_3_dep': 'Arrive_Variance_Mins_dep',
        'Arrive_Variance_Mins_3_arriv': 'Arrive_Variance_Mins_arriv',
        'Travel_Time_Mins_3_dep': 'Travel_Time_Mins_dep',
        'Travel_Time_Mins_3_arriv': 'Travel_Time_Mins_arriv',
        'Idle_Time_Mins_3_dep': 'Idle_Time_Mins_dep',
        'Idle_Time_Mins_3_arriv': 'Idle_Time_Mins_arriv',
        'KM_Distance_Event_3_dep': 'KM_Distance_Event_dep',
        'KM_Distance_Event_3_arriv': 'KM_Distance_Event_arriv',
        'KM_HR_Event_3_dep': 'KM_HR_Event_dep',
        'KM_HR_Event_3_arriv': 'KM_HR_Event_arriv'
    },
                          inplace=True)

    # data_station_4
    data_station_4 = dataset_2.copy()
    data_station_4 = data_station_4[[
        'IDTRAIN', 'Station_Name_4_dep', 'Station_Name_4_arriv',
        'Plan_Timestamp_4_dep', 'Plan_Timestamp_4_arriv',
        'Actual_Timestamp_4_dep', 'Actual_Timestamp_4_arriv',
        'Time_From_Prior_Plan_Mins_4_dep', 'Time_From_Prior_Plan_Mins_4_arriv',
        'Depart_Variance_Mins_4_dep', 'Depart_Variance_Mins_4_arriv',
        'Arrive_Variance_Mins_4_dep', 'Arrive_Variance_Mins_4_arriv',
        'Travel_Time_Mins_4_dep', 'Travel_Time_Mins_4_arriv',
        'Idle_Time_Mins_4_dep', 'Idle_Time_Mins_4_arriv',
        'KM_Distance_Event_4_dep', 'KM_Distance_Event_4_arriv',
        'KM_HR_Event_4_dep', 'KM_HR_Event_4_arriv'
    ]]
    data_station_4.rename(columns={
        'Station_Name_4_dep': 'Station_Name_dep',
        'Station_Name_4_arriv': 'Station_Name_arriv',
        'Plan_Timestamp_4_dep': 'Plan_Timestamp_dep',
        'Plan_Timestamp_4_arriv': 'Plan_Timestamp_arriv',
        'Actual_Timestamp_4_dep': 'Actual_Timestamp_dep',
        'Actual_Timestamp_4_arriv': 'Actual_Timestamp_arriv',
        'Time_From_Prior_Plan_Mins_4_dep': 'Time_From_Prior_Plan_Mins_dep',
        'Time_From_Prior_Plan_Mins_4_arriv': 'Time_From_Prior_Plan_Mins_arriv',
        'Depart_Variance_Mins_4_dep': 'Depart_Variance_Mins_dep',
        'Depart_Variance_Mins_4_arriv': 'Depart_Variance_Mins_arriv',
        'Arrive_Variance_Mins_4_dep': 'Arrive_Variance_Mins_dep',
        'Arrive_Variance_Mins_4_arriv': 'Arrive_Variance_Mins_arriv',
        'Travel_Time_Mins_4_dep': 'Travel_Time_Mins_dep',
        'Travel_Time_Mins_4_arriv': 'Travel_Time_Mins_arriv',
        'Idle_Time_Mins_4_dep': 'Idle_Time_Mins_dep',
        'Idle_Time_Mins_4_arriv': 'Idle_Time_Mins_arriv',
        'KM_Distance_Event_4_dep': 'KM_Distance_Event_dep',
        'KM_Distance_Event_4_arriv': 'KM_Distance_Event_arriv',
        'KM_HR_Event_4_dep': 'KM_HR_Event_dep',
        'KM_HR_Event_4_arriv': 'KM_HR_Event_arriv'
    },
                          inplace=True)

    # data_station_5
    data_station_5 = dataset_2.copy()
    data_station_5 = data_station_5[[
        'IDTRAIN', 'Station_Name_5_dep', 'Station_Name_5_arriv',
        'Plan_Timestamp_5_dep', 'Plan_Timestamp_5_arriv',
        'Actual_Timestamp_5_dep', 'Actual_Timestamp_5_arriv',
        'Time_From_Prior_Plan_Mins_5_dep', 'Time_From_Prior_Plan_Mins_5_arriv',
        'Depart_Variance_Mins_5_dep', 'Depart_Variance_Mins_5_arriv',
        'Arrive_Variance_Mins_5_dep', 'Arrive_Variance_Mins_5_arriv',
        'Travel_Time_Mins_5_dep', 'Travel_Time_Mins_5_arriv',
        'Idle_Time_Mins_5_dep', 'Idle_Time_Mins_5_arriv',
        'KM_Distance_Event_5_dep', 'KM_Distance_Event_5_arriv',
        'KM_HR_Event_5_dep', 'KM_HR_Event_5_arriv'
    ]]
    data_station_5.rename(columns={
        'Station_Name_5_dep': 'Station_Name_dep',
        'Station_Name_5_arriv': 'Station_Name_arriv',
        'Plan_Timestamp_5_dep': 'Plan_Timestamp_dep',
        'Plan_Timestamp_5_arriv': 'Plan_Timestamp_arriv',
        'Actual_Timestamp_5_dep': 'Actual_Timestamp_dep',
        'Actual_Timestamp_5_arriv': 'Actual_Timestamp_arriv',
        'Time_From_Prior_Plan_Mins_5_dep': 'Time_From_Prior_Plan_Mins_dep',
        'Time_From_Prior_Plan_Mins_5_arriv': 'Time_From_Prior_Plan_Mins_arriv',
        'Depart_Variance_Mins_5_dep': 'Depart_Variance_Mins_dep',
        'Depart_Variance_Mins_5_arriv': 'Depart_Variance_Mins_arriv',
        'Arrive_Variance_Mins_5_dep': 'Arrive_Variance_Mins_dep',
        'Arrive_Variance_Mins_5_arriv': 'Arrive_Variance_Mins_arriv',
        'Travel_Time_Mins_5_dep': 'Travel_Time_Mins_dep',
        'Travel_Time_Mins_5_arriv': 'Travel_Time_Mins_arriv',
        'Idle_Time_Mins_5_dep': 'Idle_Time_Mins_dep',
        'Idle_Time_Mins_5_arriv': 'Idle_Time_Mins_arriv',
        'KM_Distance_Event_5_dep': 'KM_Distance_Event_dep',
        'KM_Distance_Event_5_arriv': 'KM_Distance_Event_arriv',
        'KM_HR_Event_5_dep': 'KM_HR_Event_dep',
        'KM_HR_Event_5_arriv': 'KM_HR_Event_arriv'
    },
                          inplace=True)

    # data_station_6
    data_station_6 = dataset_2.copy()
    data_station_6 = data_station_6[[
        'IDTRAIN', 'Station_Name_6_dep', 'Station_Name_6_arriv',
        'Plan_Timestamp_6_dep', 'Plan_Timestamp_6_arriv',
        'Actual_Timestamp_6_dep', 'Actual_Timestamp_6_arriv',
        'Time_From_Prior_Plan_Mins_6_dep', 'Time_From_Prior_Plan_Mins_6_arriv',
        'Depart_Variance_Mins_6_dep', 'Depart_Variance_Mins_6_arriv',
        'Arrive_Variance_Mins_6_dep', 'Arrive_Variance_Mins_6_arriv',
        'Travel_Time_Mins_6_dep', 'Travel_Time_Mins_6_arriv',
        'Idle_Time_Mins_6_dep', 'Idle_Time_Mins_6_arriv',
        'KM_Distance_Event_6_dep', 'KM_Distance_Event_6_arriv',
        'KM_HR_Event_6_dep', 'KM_HR_Event_6_arriv'
    ]]
    data_station_6.rename(columns={
        'Station_Name_6_dep': 'Station_Name_dep',
        'Station_Name_6_arriv': 'Station_Name_arriv',
        'Plan_Timestamp_6_dep': 'Plan_Timestamp_dep',
        'Plan_Timestamp_6_arriv': 'Plan_Timestamp_arriv',
        'Actual_Timestamp_6_dep': 'Actual_Timestamp_dep',
        'Actual_Timestamp_6_arriv': 'Actual_Timestamp_arriv',
        'Time_From_Prior_Plan_Mins_6_dep': 'Time_From_Prior_Plan_Mins_dep',
        'Time_From_Prior_Plan_Mins_6_arriv': 'Time_From_Prior_Plan_Mins_arriv',
        'Depart_Variance_Mins_6_dep': 'Depart_Variance_Mins_dep',
        'Depart_Variance_Mins_6_arriv': 'Depart_Variance_Mins_arriv',
        'Arrive_Variance_Mins_6_dep': 'Arrive_Variance_Mins_dep',
        'Arrive_Variance_Mins_6_arriv': 'Arrive_Variance_Mins_arriv',
        'Travel_Time_Mins_6_dep': 'Travel_Time_Mins_dep',
        'Travel_Time_Mins_6_arriv': 'Travel_Time_Mins_arriv',
        'Idle_Time_Mins_6_dep': 'Idle_Time_Mins_dep',
        'Idle_Time_Mins_6_arriv': 'Idle_Time_Mins_arriv',
        'KM_Distance_Event_6_dep': 'KM_Distance_Event_dep',
        'KM_Distance_Event_6_arriv': 'KM_Distance_Event_arriv',
        'KM_HR_Event_6_dep': 'KM_HR_Event_dep',
        'KM_HR_Event_6_arriv': 'KM_HR_Event_arriv'
    },
                          inplace=True)

    # data_station_7
    data_station_7 = dataset_2.copy()
    data_station_7 = data_station_7[[
        'IDTRAIN', 'Station_Name_7_dep', 'Station_Name_7_arriv',
        'Plan_Timestamp_7_dep', 'Plan_Timestamp_7_arriv',
        'Actual_Timestamp_7_dep', 'Actual_Timestamp_7_arriv',
        'Time_From_Prior_Plan_Mins_7_dep', 'Time_From_Prior_Plan_Mins_7_arriv',
        'Depart_Variance_Mins_7_dep', 'Depart_Variance_Mins_7_arriv',
        'Arrive_Variance_Mins_7_dep', 'Arrive_Variance_Mins_7_arriv',
        'Travel_Time_Mins_7_dep', 'Travel_Time_Mins_7_arriv',
        'Idle_Time_Mins_7_dep', 'Idle_Time_Mins_7_arriv',
        'KM_Distance_Event_7_dep', 'KM_Distance_Event_7_arriv',
        'KM_HR_Event_7_dep', 'KM_HR_Event_7_arriv'
    ]]
    data_station_7.rename(columns={
        'Station_Name_7_dep': 'Station_Name_dep',
        'Station_Name_7_arriv': 'Station_Name_arriv',
        'Plan_Timestamp_7_dep': 'Plan_Timestamp_dep',
        'Plan_Timestamp_7_arriv': 'Plan_Timestamp_arriv',
        'Actual_Timestamp_7_dep': 'Actual_Timestamp_dep',
        'Actual_Timestamp_7_arriv': 'Actual_Timestamp_arriv',
        'Time_From_Prior_Plan_Mins_7_dep': 'Time_From_Prior_Plan_Mins_dep',
        'Time_From_Prior_Plan_Mins_7_arriv': 'Time_From_Prior_Plan_Mins_arriv',
        'Depart_Variance_Mins_7_dep': 'Depart_Variance_Mins_dep',
        'Depart_Variance_Mins_7_arriv': 'Depart_Variance_Mins_arriv',
        'Arrive_Variance_Mins_7_dep': 'Arrive_Variance_Mins_dep',
        'Arrive_Variance_Mins_7_arriv': 'Arrive_Variance_Mins_arriv',
        'Travel_Time_Mins_7_dep': 'Travel_Time_Mins_dep',
        'Travel_Time_Mins_7_arriv': 'Travel_Time_Mins_arriv',
        'Idle_Time_Mins_7_dep': 'Idle_Time_Mins_dep',
        'Idle_Time_Mins_7_arriv': 'Idle_Time_Mins_arriv',
        'KM_Distance_Event_7_dep': 'KM_Distance_Event_dep',
        'KM_Distance_Event_7_arriv': 'KM_Distance_Event_arriv',
        'KM_HR_Event_7_dep': 'KM_HR_Event_dep',
        'KM_HR_Event_7_arriv': 'KM_HR_Event_arriv'
    },
                          inplace=True)

    # data_station_8
    data_station_8 = dataset_2.copy()
    data_station_8 = data_station_8[[
        'IDTRAIN', 'Station_Name_8_dep', 'Station_Name_8_arriv',
        'Plan_Timestamp_8_dep', 'Plan_Timestamp_8_arriv',
        'Actual_Timestamp_8_dep', 'Actual_Timestamp_8_arriv',
        'Time_From_Prior_Plan_Mins_8_dep', 'Time_From_Prior_Plan_Mins_8_arriv',
        'Depart_Variance_Mins_8_dep', 'Depart_Variance_Mins_8_arriv',
        'Arrive_Variance_Mins_8_dep', 'Arrive_Variance_Mins_8_arriv',
        'Travel_Time_Mins_8_dep', 'Travel_Time_Mins_8_arriv',
        'Idle_Time_Mins_8_dep', 'Idle_Time_Mins_8_arriv',
        'KM_Distance_Event_8_dep', 'KM_Distance_Event_8_arriv',
        'KM_HR_Event_8_dep', 'KM_HR_Event_8_arriv'
    ]]
    data_station_8.rename(columns={
        'Station_Name_8_dep': 'Station_Name_dep',
        'Station_Name_8_arriv': 'Station_Name_arriv',
        'Plan_Timestamp_8_dep': 'Plan_Timestamp_dep',
        'Plan_Timestamp_8_arriv': 'Plan_Timestamp_arriv',
        'Actual_Timestamp_8_dep': 'Actual_Timestamp_dep',
        'Actual_Timestamp_8_arriv': 'Actual_Timestamp_arriv',
        'Time_From_Prior_Plan_Mins_8_dep': 'Time_From_Prior_Plan_Mins_dep',
        'Time_From_Prior_Plan_Mins_8_arriv': 'Time_From_Prior_Plan_Mins_arriv',
        'Depart_Variance_Mins_8_dep': 'Depart_Variance_Mins_dep',
        'Depart_Variance_Mins_8_arriv': 'Depart_Variance_Mins_arriv',
        'Arrive_Variance_Mins_8_dep': 'Arrive_Variance_Mins_dep',
        'Arrive_Variance_Mins_8_arriv': 'Arrive_Variance_Mins_arriv',
        'Travel_Time_Mins_8_dep': 'Travel_Time_Mins_dep',
        'Travel_Time_Mins_8_arriv': 'Travel_Time_Mins_arriv',
        'Idle_Time_Mins_8_dep': 'Idle_Time_Mins_dep',
        'Idle_Time_Mins_8_arriv': 'Idle_Time_Mins_arriv',
        'KM_Distance_Event_8_dep': 'KM_Distance_Event_dep',
        'KM_Distance_Event_8_arriv': 'KM_Distance_Event_arriv',
        'KM_HR_Event_8_dep': 'KM_HR_Event_dep',
        'KM_HR_Event_8_arriv': 'KM_HR_Event_arriv'
    },
                          inplace=True)

    # data_station_9
    data_station_9 = dataset_2.copy()
    data_station_9 = data_station_9[[
        'IDTRAIN', 'Station_Name_9_dep', 'Station_Name_9_arriv',
        'Plan_Timestamp_9_dep', 'Plan_Timestamp_9_arriv',
        'Actual_Timestamp_9_dep', 'Actual_Timestamp_9_arriv',
        'Time_From_Prior_Plan_Mins_9_dep', 'Time_From_Prior_Plan_Mins_9_arriv',
        'Depart_Variance_Mins_9_dep', 'Depart_Variance_Mins_9_arriv',
        'Arrive_Variance_Mins_9_dep', 'Arrive_Variance_Mins_9_arriv',
        'Travel_Time_Mins_9_dep', 'Travel_Time_Mins_9_arriv',
        'Idle_Time_Mins_9_dep', 'Idle_Time_Mins_9_arriv',
        'KM_Distance_Event_9_dep', 'KM_Distance_Event_9_arriv',
        'KM_HR_Event_9_dep', 'KM_HR_Event_9_arriv'
    ]]
    data_station_9.rename(columns={
        'Station_Name_9_dep': 'Station_Name_dep',
        'Station_Name_9_arriv': 'Station_Name_arriv',
        'Plan_Timestamp_9_dep': 'Plan_Timestamp_dep',
        'Plan_Timestamp_9_arriv': 'Plan_Timestamp_arriv',
        'Actual_Timestamp_9_dep': 'Actual_Timestamp_dep',
        'Actual_Timestamp_9_arriv': 'Actual_Timestamp_arriv',
        'Time_From_Prior_Plan_Mins_9_dep': 'Time_From_Prior_Plan_Mins_dep',
        'Time_From_Prior_Plan_Mins_9_arriv': 'Time_From_Prior_Plan_Mins_arriv',
        'Depart_Variance_Mins_9_dep': 'Depart_Variance_Mins_dep',
        'Depart_Variance_Mins_9_arriv': 'Depart_Variance_Mins_arriv',
        'Arrive_Variance_Mins_9_dep': 'Arrive_Variance_Mins_dep',
        'Arrive_Variance_Mins_9_arriv': 'Arrive_Variance_Mins_arriv',
        'Travel_Time_Mins_9_dep': 'Travel_Time_Mins_dep',
        'Travel_Time_Mins_9_arriv': 'Travel_Time_Mins_arriv',
        'Idle_Time_Mins_9_dep': 'Idle_Time_Mins_dep',
        'Idle_Time_Mins_9_arriv': 'Idle_Time_Mins_arriv',
        'KM_Distance_Event_9_dep': 'KM_Distance_Event_dep',
        'KM_Distance_Event_9_arriv': 'KM_Distance_Event_arriv',
        'KM_HR_Event_9_dep': 'KM_HR_Event_dep',
        'KM_HR_Event_9_arriv': 'KM_HR_Event_arriv'
    },
                          inplace=True)

    # data_station_10
    data_station_10 = dataset_2.copy()
    data_station_10 = data_station_10[[
        'IDTRAIN', 'Station_Name_10_dep', 'Station_Name_10_arriv',
        'Plan_Timestamp_10_dep', 'Plan_Timestamp_10_arriv',
        'Actual_Timestamp_10_dep', 'Actual_Timestamp_10_arriv',
        'Time_From_Prior_Plan_Mins_10_dep',
        'Time_From_Prior_Plan_Mins_10_arriv', 'Depart_Variance_Mins_10_dep',
        'Depart_Variance_Mins_10_arriv', 'Arrive_Variance_Mins_10_dep',
        'Arrive_Variance_Mins_10_arriv', 'Travel_Time_Mins_10_dep',
        'Travel_Time_Mins_10_arriv', 'Idle_Time_Mins_10_dep',
        'Idle_Time_Mins_10_arriv', 'KM_Distance_Event_10_dep',
        'KM_Distance_Event_10_arriv', 'KM_HR_Event_10_dep',
        'KM_HR_Event_10_arriv'
    ]]
    data_station_10.rename(columns={
        'Station_Name_10_dep': 'Station_Name_dep',
        'Station_Name_10_arriv': 'Station_Name_arriv',
        'Plan_Timestamp_10_dep': 'Plan_Timestamp_dep',
        'Plan_Timestamp_10_arriv': 'Plan_Timestamp_arriv',
        'Actual_Timestamp_10_dep': 'Actual_Timestamp_dep',
        'Actual_Timestamp_10_arriv': 'Actual_Timestamp_arriv',
        'Time_From_Prior_Plan_Mins_10_dep': 'Time_From_Prior_Plan_Mins_dep',
        'Time_From_Prior_Plan_Mins_10_arriv':
        'Time_From_Prior_Plan_Mins_arriv',
        'Depart_Variance_Mins_10_dep': 'Depart_Variance_Mins_dep',
        'Depart_Variance_Mins_10_arriv': 'Depart_Variance_Mins_arriv',
        'Arrive_Variance_Mins_10_dep': 'Arrive_Variance_Mins_dep',
        'Arrive_Variance_Mins_10_arriv': 'Arrive_Variance_Mins_arriv',
        'Travel_Time_Mins_10_dep': 'Travel_Time_Mins_dep',
        'Travel_Time_Mins_10_arriv': 'Travel_Time_Mins_arriv',
        'Idle_Time_Mins_10_dep': 'Idle_Time_Mins_dep',
        'Idle_Time_Mins_10_arriv': 'Idle_Time_Mins_arriv',
        'KM_Distance_Event_10_dep': 'KM_Distance_Event_dep',
        'KM_Distance_Event_10_arriv': 'KM_Distance_Event_arriv',
        'KM_HR_Event_10_dep': 'KM_HR_Event_dep',
        'KM_HR_Event_10_arriv': 'KM_HR_Event_arriv'
    },
                           inplace=True)

    # data_station_11
    data_station_11 = dataset_2.copy()
    data_station_11 = data_station_11[[
        'IDTRAIN', 'Station_Name_11_dep', 'Station_Name_11_arriv',
        'Plan_Timestamp_11_dep', 'Plan_Timestamp_11_arriv',
        'Actual_Timestamp_11_dep', 'Actual_Timestamp_11_arriv',
        'Time_From_Prior_Plan_Mins_11_dep',
        'Time_From_Prior_Plan_Mins_11_arriv', 'Depart_Variance_Mins_11_dep',
        'Depart_Variance_Mins_11_arriv', 'Arrive_Variance_Mins_11_dep',
        'Arrive_Variance_Mins_11_arriv', 'Travel_Time_Mins_11_dep',
        'Travel_Time_Mins_11_arriv', 'Idle_Time_Mins_11_dep',
        'Idle_Time_Mins_11_arriv', 'KM_Distance_Event_11_dep',
        'KM_Distance_Event_11_arriv', 'KM_HR_Event_11_dep',
        'KM_HR_Event_11_arriv'
    ]]
    data_station_11.rename(columns={
        'Station_Name_11_dep': 'Station_Name_dep',
        'Station_Name_11_arriv': 'Station_Name_arriv',
        'Plan_Timestamp_11_dep': 'Plan_Timestamp_dep',
        'Plan_Timestamp_11_arriv': 'Plan_Timestamp_arriv',
        'Actual_Timestamp_11_dep': 'Actual_Timestamp_dep',
        'Actual_Timestamp_11_arriv': 'Actual_Timestamp_arriv',
        'Time_From_Prior_Plan_Mins_11_dep': 'Time_From_Prior_Plan_Mins_dep',
        'Time_From_Prior_Plan_Mins_11_arriv':
        'Time_From_Prior_Plan_Mins_arriv',
        'Depart_Variance_Mins_11_dep': 'Depart_Variance_Mins_dep',
        'Depart_Variance_Mins_11_arriv': 'Depart_Variance_Mins_arriv',
        'Arrive_Variance_Mins_11_dep': 'Arrive_Variance_Mins_dep',
        'Arrive_Variance_Mins_11_arriv': 'Arrive_Variance_Mins_arriv',
        'Travel_Time_Mins_11_dep': 'Travel_Time_Mins_dep',
        'Travel_Time_Mins_11_arriv': 'Travel_Time_Mins_arriv',
        'Idle_Time_Mins_11_dep': 'Idle_Time_Mins_dep',
        'Idle_Time_Mins_11_arriv': 'Idle_Time_Mins_arriv',
        'KM_Distance_Event_11_dep': 'KM_Distance_Event_dep',
        'KM_Distance_Event_11_arriv': 'KM_Distance_Event_arriv',
        'KM_HR_Event_11_dep': 'KM_HR_Event_dep',
        'KM_HR_Event_11_arriv': 'KM_HR_Event_arriv'
    },
                           inplace=True)

    # data_station_12
    data_station_12 = dataset_2.copy()
    data_station_12 = data_station_12[[
        'IDTRAIN', 'Station_Name_12_dep', 'Station_Name_12_arriv',
        'Plan_Timestamp_12_dep', 'Plan_Timestamp_12_arriv',
        'Actual_Timestamp_12_dep', 'Actual_Timestamp_12_arriv',
        'Time_From_Prior_Plan_Mins_12_dep',
        'Time_From_Prior_Plan_Mins_12_arriv', 'Depart_Variance_Mins_12_dep',
        'Depart_Variance_Mins_12_arriv', 'Arrive_Variance_Mins_12_dep',
        'Arrive_Variance_Mins_12_arriv', 'Travel_Time_Mins_12_dep',
        'Travel_Time_Mins_12_arriv', 'Idle_Time_Mins_12_dep',
        'Idle_Time_Mins_12_arriv', 'KM_Distance_Event_12_dep',
        'KM_Distance_Event_12_arriv', 'KM_HR_Event_12_dep',
        'KM_HR_Event_12_arriv'
    ]]
    data_station_12.rename(columns={
        'Station_Name_12_dep': 'Station_Name_dep',
        'Station_Name_12_arriv': 'Station_Name_arriv',
        'Plan_Timestamp_12_dep': 'Plan_Timestamp_dep',
        'Plan_Timestamp_12_arriv': 'Plan_Timestamp_arriv',
        'Actual_Timestamp_12_dep': 'Actual_Timestamp_dep',
        'Actual_Timestamp_12_arriv': 'Actual_Timestamp_arriv',
        'Time_From_Prior_Plan_Mins_12_dep': 'Time_From_Prior_Plan_Mins_dep',
        'Time_From_Prior_Plan_Mins_12_arriv':
        'Time_From_Prior_Plan_Mins_arriv',
        'Depart_Variance_Mins_12_dep': 'Depart_Variance_Mins_dep',
        'Depart_Variance_Mins_12_arriv': 'Depart_Variance_Mins_arriv',
        'Arrive_Variance_Mins_12_dep': 'Arrive_Variance_Mins_dep',
        'Arrive_Variance_Mins_12_arriv': 'Arrive_Variance_Mins_arriv',
        'Travel_Time_Mins_12_dep': 'Travel_Time_Mins_dep',
        'Travel_Time_Mins_12_arriv': 'Travel_Time_Mins_arriv',
        'Idle_Time_Mins_12_dep': 'Idle_Time_Mins_dep',
        'Idle_Time_Mins_12_arriv': 'Idle_Time_Mins_arriv',
        'KM_Distance_Event_12_dep': 'KM_Distance_Event_dep',
        'KM_Distance_Event_12_arriv': 'KM_Distance_Event_arriv',
        'KM_HR_Event_12_dep': 'KM_HR_Event_dep',
        'KM_HR_Event_12_arriv': 'KM_HR_Event_arriv'
    },
                           inplace=True)

    df_final = pd.concat([
        data_station_1, data_station_2, data_station_3, data_station_4,
        data_station_5, data_station_6, data_station_7, data_station_8,
        data_station_9, data_station_10, data_station_11, data_station_12
    ],
                         ignore_index=True)
    df_final = dataset_1.merge(df_final, on=['IDTRAIN'], how='left')

    # Dropping columns without data
    df_final.drop(columns=[
        'Depart_Variance_Mins_arriv', 'Arrive_Variance_Mins_dep',
        'Travel_Time_Mins_dep', 'Idle_Time_Mins_arriv',
        'KM_Distance_Event_dep', 'KM_HR_Event_dep'
    ],
                  axis=1,
                  inplace=True)

    # Cleaning rows without ORIGIN
    df_final['Station_Name_dep'].fillna(False, inplace=True)
    df_final = df_final[df_final['Station_Name_dep'] != False].reset_index(
        drop=True)

    ##################
    # Export
    ##################
    df_final.to_csv(df_final_file, index=False)
    routes.to_csv(routes_file, index=False)


if __name__ == "__main__":
    preparing()