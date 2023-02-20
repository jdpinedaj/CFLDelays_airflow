# -*- coding: utf-8 -*-
"""
This script is used to run convert the raw data to train and test data
It is designed to be idempotent [stateless transformation]
Usage:
    python ./scripts/etl.py
"""

from pathlib import Path
import click
import pandas as pd
import numpy as np
import calendar
import datetime as dt
import ppscore as pps
import scipy.stats as stats
from utility import parse_config, set_logger, time_train


@click.command()
@click.argument("config_file", type=str, default="scripts/config.yml")
def etl(config_file):
    """
    ETL function that load raw data and convert to train and test set
    Args:
        config_file [str]: path to config file
    Returns:
        None
    """

    ##################
    # configure logger
    ##################
    logger = set_logger("./script_logs/etl.log")

    ##################
    # Load config from config file
    ##################
    logger.info(f"Load config from {config_file}")
    config = parse_config(config_file)

    raw_data_file = config["etl"]["raw_data_file"]
    processed_path = Path(config["etl"]["processed_path"])
    #test_size = config["etl"]["test_size"]
    #random_state = config["etl"]["random_state"]
    logger.info(f"config: {config['etl']}")

    ##################
    # Reading data
    ##################
    df = pd.read_csv(raw_data_file, low_memory=False)

    ##################
    # Data transformation and Feature engineering
    ##################
    logger.info(
        "-------------------Start data transformation and feature engineering-------------------"
    )

    # Conversion of some features
    df = df.replace(['False', False], np.nan)
    df['Train_Weight'] = df['Train_Weight'] / 1000  # Converting units to t
    df['weight_length'] = df['Train_Weight'] / df['Train_Length']  # units= t/m
    df['weight_wagon'] = df['Train_Weight'] / df[
        'wagon_count']  # units= t/wagon
    df['IDTRAIN'] = df['IDTRAIN'].astype(str)
    df = df[df['Origin_Station'] != 'Mertert-Port'].reset_index(drop=True)
    df = df[df['Destination_Station'] != 'Mertert-Port'].reset_index(drop=True)
    logger.info(f"shape: {df.shape}")

    logger.info("Filtering canceled trains.")
    # Filtering canceled trains
    df = df[df['Cancelled_Train_Bin'] == 0].reset_index(drop=True)

    # Creating data based on stations' data.
    logger.info(
        "Creating a dictionary of countries and coordinates based on stations' data."
    )
    dict_country = {
        'Antwerp': ['Belgium', '51.371889', '4.276158'],
        'Antwerpen-Noord N': ['Belgium', '51.257856', '4.366801'],
        'Apach-Frontiere': ['France', '49.468881', '6.369765'],
        'Athus': ['Luxembourg', '49.551688', '5.824975'],
        'BERTRIX': ['Belgium', '49.85070000', '5.26894000'],
        'Berchem': ['Luxembourg', '49.542689', '6.133661'],
        'Bettembourg': ['Luxembourg', '49.493889', '6.108889'],
        'Bettembourg-frontière': ['Luxembourg', '49.46785900', '6.10915300'],
        'Bietigheim-Bissingen': ['Germany', '48.947986', '9.137338'],
        'Bischofshofen': ['Austria', '47.417438', '13.219925'],
        'Boulou-Perthus': ['France', '42.525299', '2.816646'],
        'Brennero (Brenner)': ['Italy', '47.002378', '11.505089'],
        'Brennero/Brenner': ['Austria', '47.007338', '11.507839'],
        'Brennero/Brenner-Übergang': ['Austria', '47.00194000', '11.50529300'],
        'Brugge': ['Belgium', '51.197517', '3.217157'],
        'Champigneulles': ['France', '48.736397', '6.167817'],
        'Dieulouard': ['France', '48.84307800', '6.07213000'],
        'Duisburg-Wedau': ['Germany', '51.42775200', '6.77576600'],
        'Ehrang': ['Germany', '49.80200000', '6.68600000'],
        'Erfurt-Vieselbach Ubf': ['Germany', '50.990367', '11.125075'],
        'Esch-sur-Alzette': ['Luxembourg', '49.49466900', '5.98666200'],
        'Forbach': ['France', '49.189505', '6.900831'],
        'Forbach-Frontiere': ['France', '49.19390900', '6.91057000'],
        'Frankfurt (Oder) Oderbrücke': ['Germany', '52.336830', '14.546645'],
        'Frouard': ['France', '48.75520900', '6.14507600'],
        'Fulda': ['Germany', '50.554845', '9.684087'],
        'Gembloux': ['Belgium', '50.570490', '4.691746'],
        'Gent': ['Belgium', '51.06585800', '3.74228800'],
        'Gorizia Centrale': ['Italy', '45.927263', '13.610594'],
        'Gremberg': ['Germany', '50.931872', '7.004873'],
        'Grignano': ['Italy', '45.712456', '13.712081'],
        'Gummern': ['Austria', '46.654821', '13.768172'],
        'Hagondange': ['France', '49.24571', '6.16293300'],
        'Hamburg Harburg': ['Germany', '53.456259', '9.991911'],
        'Hamburg-Barmbek': ['Germany', '53.587298', '10.044225'],
        'Hamburg-Billwerder Ubf': ['Germany', '53.515913', '10.095863'],
        'Hamburg-Eidelstedt': ['Germany', '53.606569', '9.881261'],
        'Hamm (Westf) Rbf': ['Germany', '51.678478', '7.808450'],
        'Hanau Nord': ['Germany', '50.141900', '8.925691'],
        'Harburg (Schwab)': ['Germany', '48.778772', '10.698509'],
        'Igel Grenze': ['Germany', '49.70827000', '6.54751000'],
        'Innsbruck-Hbf': ['Austria', '47.263270', '11.401065'],
        'Kiel': ['Germany', '54.313586', '10.131083'],
        'Kiel-Meimersdorf': ['Germany', '54.28598400', '10.11431200'],
        'Kirchweyhe': ['Germany', '52.983717', '8.847205'],
        'Koblenz Mosel Gbf': ['Germany', '50.358060', '7.590114'],
        'Kornwestheim Ubf': ['Germany', '48.862236', '9.179783'],
        'Kufstein': ['Austria', '47.58351300', '12.16554500'],
        'Kufstein-Transit': ['Austria', '47.601123', '12.177298'],
        'Köln Eifeltor': ['Germany', '50.906783', '6.931220'],
        'Launsdorf-Hochosterwitz': ['Austria', '46.769031', '14.455553'],
        'Leuven': ['Belgium', '50.881412', '4.716230'],
        'Linz (Rhein)': ['Germany', '50.569312', '7.275979'],
        'Lyon': ['France', '45.738545', '4.847001'],
        'Löhne (Westf)': ['Germany', '52.196869', '8.713322'],
        'Mainz Hbf': ['Germany', '50.001397', '8.258400'],
        'Marl-Sinsen': ['Germany', '51.667738', '7.173067'],
        'Maschen Rbf': ['Germany', '53.40597900', '10.05616000'],
        'Mertert-Port': ['Luxembourg', '49.69852900', '6.47328700'],
        'Metz-Chambiere': ['France', '49.12633600', '6.15989800'],
        'Metz-Sablon': ['France', '49.09640700', '6.16632000'],
        'Monceau': ['Belgium', '50.412302', '4.394469'],
        'Monfalcone': ['Italy', '45.807600', '13.543315'],
        'Mont-Saint-Martin-(Fre-Bel)': ['France', '49.539485', '5.795765'],
        'Muizen': ['Belgium', '51.008260', '4.513946'],
        'München-Laim': ['Germany', '48.15053500', '11.46107900'],
        'Münster (Westf) Ost': ['Germany', '51.956909', '7.635720'],
        'Neumünster': ['Germany', '54.076277', '9.980326'],
        'Neuoffingen': ['Germany', '48.482938', '10.345244'],
        'Neustadt (Schwarzw)': ['Germany', '47.910334', '8.210948'],
        'Neuwied': ['Germany', '50.431505', '7.473153'],
        'Ochsenfurt': ['Germany', '49.663138', '10.071667'],
        'Osnabrück': ['Germany', '52.27287500', '8.06157700'],
        'Perrigny': ['France', '47.29046800', '5.02924200'],
        'Plessis-Belleville-(Le)': ['France', '49.09817600', '2.74655600'],
        'Pont-A-Mousson': ['France', '48.900174', '6.050552'],
        'Port-La-Nouvelle': ['France', '43.019999', '3.038663'],
        'Poznan': ['Poland', '52.402759', '17.095353'],
        'RZEPIN': ['Poland', '52.350125', '14.815257'],
        'Raubling': ['Germany', '47.788421', '12.110035'],
        'Rodange': ['Luxembourg', '49.551095', '5.843570'],
        'Rodange-Athus-frontière': ['Luxembourg', '49.551665', '5.824990'],
        'Rodange-Aubange-frontière':
        ['Luxembourg', '49.56265800', '5.80172000'],
        'Ronet': ['Belgium', '50.457953', '4.829284'],
        'Rostock': ['Germany', '54.150763977112', '12.1002551362189'],
        'Saarbrücken Rbf': ['Germany', '49.24208900', '6.97109700'],
        'Salzburg Hbf': ['Germany', '47.80555200', '13.03289500'],
        'Salzburg-Hbf': ['Austria', '47.813024', '13.045337'],
        'Schwarzenbach': ['Germany', '49.724680', '11.992859'],
        'Schwerin-Goerris': ['Germany', '53.608673', '11.384560'],
        'Sibelin': ['France', '46.650792', '4.838708'],
        "St-Germain-Au-Mont-D'Or": ['France', '45.888651', '4.804246'],
        'Tarvisio Boscoverde': ['Italy', '46.506276', '13.607397'],
        'Tarvisio-Boscoverde': ['Austria', '46.540147', '13.648156'],
        'Thionville': ['France', '49.35566700', '6.17307200'],
        'Toul': ['France', '48.679211', '5.880462'],
        'Treuchtlingen': ['Germany', '48.961132', '10.908102'],
        'Trieste': ['Italy', '45.639233', '13.755926'],
        'Udine': ['Italy', '46.055684', '13.242007'],
        'Ulm Ubf': ['Germany', '48.399482', '9.982538'],
        'Verona Porta Nuova Scalo': ['Italy', '45.428674', '10.982622'],
        'Villach-Westbahnhof': ['Austria', '46.608000', '13.839648'],
        'Wasserbillig': ['Luxembourg', '49.71361100', '6.50638900'],
        'Wattenbek': ['Germany', '54.174414', '10.043755'],
        'Woippy': ['France', '49.14951200', '6.15606100'],
        'Zeebrugge': ['Belgium', '51.34017', '3.221882'],
        'Zoufftgen-Frontiere': ['France', '49.46785900', '6.10915300'],
    }

    stations_countries = pd.DataFrame({
        'station': [key for key in dict_country.keys()],
        'country': [dict_country[x][0] for x in dict_country],
        'lat': [dict_country[x][1] for x in dict_country],
        'long': [dict_country[x][2] for x in dict_country]
    })
    stations_countries['lat'] = stations_countries['lat'].astype(float)
    stations_countries['long'] = stations_countries['long'].astype(float)

    ## Adding departure and arrival data
    # Country_dep
    df = pd.merge(df,
                  stations_countries,
                  left_on=['Station_Name_dep'],
                  right_on=['station'],
                  how='left')

    # Country_arriv
    df = pd.merge(df,
                  stations_countries,
                  left_on=['Station_Name_arriv'],
                  right_on=['station'],
                  how='left')

    df = df.rename(
        columns={
            'station_x': 'station_dep',
            'station_y': 'station_arriv',
            'country_x': 'Country_dep',
            'country_y': 'Country_arriv',
            'lat_x': 'lat_dep',
            'lat_y': 'lat_arriv',
            'long_x': 'long_dep',
            'long_y': 'long_arriv',
        })

    df['route'] = df['Station_Name_dep'] + '--' + df['Station_Name_arriv']
    df = df.drop(columns=['station_dep', 'station_arriv'])

    logger.info("End data transformation and feature engineering")

    ##################
    # Feature selection
    ##################

    logger.info(
        "-------------------Start feature selection-------------------")

    data = df.copy()
    data = data[[  #'IDTRAIN',
        'Incoterm',
        'Max_TEU',
        'TEU_Count',
        'Max_Length',
        'Train_Length',
        'Train_Weight',
        'Planned_Departure_DOW',
        'Planned_Arrival_DOW',
        'Planned_Arrival',
        'Depart_Week_Num',
        'wagon_count',
        'Train_Distance_KM',  # Be careful, this distance is a virtual distance calculated using haversine method, is not the actual disance of the track...
        'train_tare_weight',  # Please review if this value can be duplicated to all rows using the IDTRAIN
        #'precipIntensity_final_station',       # Only in the destination, not the final station of the arc
        #'precipProbability_final_station',     # Only in the destination, not the final station of the arc
        #'temperature_final_station',           # Only in the destination, not the final station of the arc
        #'apparentTemperature_final_station',   # Only in the destination, not the final station of the arc
        #'dewPoint_final_station',              # Only in the destination, not the final station of the arc
        #'humidity_final_station',              # Only in the destination, not the final station of the arc
        #'windSpeed_final_station',             # Only in the destination, not the final station of the arc
        #'windBearing_final_station',           # Only in the destination, not the final station of the arc
        #'cloudCover_final_station',            # Only in the destination, not the final station of the arc
        #'uvIndex_final_station',               # Only in the destination, not the final station of the arc
        #'visibility_final_station',            # Only in the destination, not the final station of the arc
        #'Station_Name_dep',
        #'Station_Name_arriv',
        'Country_dep',
        'Country_arriv',
        #'route',
        #'Time_From_Prior_dep',
        #'Time_From_Prior_arriv',
        #'Depart_Variance',
        'Depart_Variance_Mins_dep',
        #'Depart_Variance_Mins_Pos',
        #'Arrive_Variance',
        'Arrive_Variance_Mins_arriv',  # Feature to predict? -> Retraso en minutos en la llegada en la estacion de llegada 
        #'Arrive_Variance_Mins_Pos',
        #'Travel_Time_Mins_arriv',              ### COMPLETELY UNRELIABLE FEATURE
        #'Idle_Time_Mins_dep',
        'KM_Distance_Event_arriv',  # Be careful, this distance is a virtual distance calculated using haversine method, is not the actual disance of the track...
        'weight_length',
        'weight_wagon',
        #'type_incident',
        #'dateh_incident',
        #'lieu',
        #'statut',
        #'statut_commercial',
        #'statut_financier',
        #'gravite',
        #'motif_client',
        #'commentaire',
    ]]

    data.rename(
        columns={
            'Incoterm': 'incoterm',
            'Max_TEU': 'max_teu',
            'TEU_Count': 'teu_count',
            'Max_Length': 'max_length',
            'Train_Length': 'train_length',
            'Train_Weight': 'train_weight',
            'Planned_Departure_DOW': 'planned_departure_day',
            'Planned_Arrival_DOW': 'planned_arrival_day',
            'Planned_Arrival': 'planned_arrival',
            'Depart_Week_Num': 'departure_week_number',
            'wagon_count': 'wagon_count',
            'Train_Distance_KM': 'total_distance_trip',
            'train_tare_weight': 'sum_tares_wagons',
            'Station_Name_dep': 'departure_station',
            'Station_Name_arriv': 'arrival_station',
            'Country_dep': 'departure_country',
            'Country_arriv': 'arrival_country',
            'route': 'departure_arrival_route',
            'Depart_Variance_Mins_dep': 'departure_delay',
            'Arrive_Variance_Mins_arriv': 'arrival_delay',
            'KM_Distance_Event_arriv': 'distance_between_control_stations',
            'weight_length': 'weight_per_length_of_train',
            'weight_wagon': 'weight_per_wagon_of_train',
            #'type_incident': 'incident_type',
            #'dateh_incident': 'incident_date',
            #'lieu': 'incident_location',
            #'statut': 'incident_status',
            #'statut_commercial': 'incident_status_commercial',
            #'statut_financier': 'incident_status_financial',
            #'gravite': 'incident_gravity',
            #'motif_client': 'incident_customer_reason',
            #'commentaire': 'incident_comment',
        },
        inplace=True)

    ## Adding time data

    data['planned_arrival'] = pd.to_datetime(data['planned_arrival'])
    data['month_arrival'] = data['planned_arrival'].dt.month
    data['month_arrival'] = data['month_arrival'].apply(
        lambda x: calendar.month_abbr[x])
    data['hour_arrival'] = data['planned_arrival'].dt.hour
    data['arrival_night'] = [
        'no' if x <= 19 and x >= 6 else 'yes' for x in data['hour_arrival']
    ]
    data['peak_morning'] = [
        'yes' if x <= 9 and x >= 6 else 'no' for x in data['hour_arrival']
    ]
    data['peak_evening'] = [
        'yes' if x <= 19 and x >= 16 else 'no' for x in data['hour_arrival']
    ]
    data['peak_time'] = [
        'yes' if i == 'yes' or j == 'yes' else 'no'
        for i, j in zip(data['peak_morning'], data['peak_evening'])
    ]

    data = data.drop(columns=[
        'planned_arrival', 'hour_arrival', 'peak_morning', 'peak_evening'
    ],
                     axis=1)

    logger.info(f"data: {data.shape}")

    logger.info("End feature selection")

    ##################
    # Data cleaning
    ##################

    logger.info("-------------------Start data cleaning-------------------")

    df = data.copy()
    df.dropna(subset=['arrival_delay'], inplace=True)
    #df = df.fillna({
    #    'incident_type': 'no_incident',
    #    'incident_gravity': 'no_incident',
    #    'incident_customer_reason': 'no_incident'
    #})
    df.reset_index(drop=True, inplace=True)

    ## Dividing dataset in numeric and categorical features

    # Categorical features
    cat_features = [
        'incoterm',
        'planned_departure_day',
        'planned_arrival_day',
        'departure_country',
        'arrival_country',
        'month_arrival',
        'arrival_night',
        'peak_time',
        #'incident_type', 'incident_gravity', 'incident_customer_reason'
    ]
    cat_feat_train_data = df[cat_features]
    cat_feat_train_data = cat_feat_train_data.dropna().reset_index(drop=True)
    # Transforming categorical features
    cat_feat_train_data_cat = cat_feat_train_data.copy()
    for i in cat_feat_train_data_cat:
        cat_feat_train_data_cat[i] = cat_feat_train_data_cat[i].astype(
            'category')
        #cat_feat_train_data_cat[i] = cat_feat_train_data_cat[i].cat.codes

    # Numerical features
    num_feat_train_data = df.drop(columns=cat_features)
    num_feat_train_data = num_feat_train_data.dropna().reset_index(
        drop=True).astype(float)
    # Removing outliers of numerical features
    # find Q1, Q3, and interquartile range for each column
    Q1 = num_feat_train_data.quantile(q=0.03)
    Q3 = num_feat_train_data.quantile(q=0.97)
    IQR = num_feat_train_data.apply(stats.iqr)
    # only keep rows in dataframe that have values within 1.5*IQR of Q1 and Q3
    num_feat_train_data = num_feat_train_data[~(
        (num_feat_train_data < (Q1 - 1.5 * IQR)) |
        (num_feat_train_data >
         (Q3 + 1.5 * IQR))).any(axis=1)].reset_index(drop=True)

    # Merging categorical and numerical features
    merged_data = pd.concat([cat_feat_train_data_cat, num_feat_train_data],
                            axis=1)
    merged_data = merged_data.dropna()
    merged_data['arrived'] = merged_data['arrival_delay'].apply(
        lambda x: time_train(x))

    # Calculating PPS matrix to remove some features
    predictors = pps.predictors(merged_data,
                                'arrived',
                                output="df",
                                sorted=True)
    predictors_to_remove = predictors[
        predictors['ppscore'] <
        0.003]  # TODO: Constant for this hard-code (0.003)

    df = merged_data.drop(predictors_to_remove['x'].values, axis=1)
    df.drop(columns=['train_weight', 'sum_tares_wagons'], axis=1, inplace=True)

    # Removing this column for training regression models
    df.drop(columns=['arrived'], axis=1, inplace=True)

    logger.info("End data cleaning")

    ##################
    # Export
    ##################
    logger.info("-------------------Export-------------------")
    logger.info(f"write data to {processed_path}")
    df.to_csv(processed_path / 'df_processed_ml.csv', index=False)
    logger.info(f"df_processed_ml: {df.shape}")
    logger.info("\n")


if __name__ == "__main__":
    etl()