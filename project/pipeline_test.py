import os
from pathlib import Path
import shutil
import sqlite3
import pandas as pd
import numpy as np
import pytest
import geopandas as gpd
from shapely.geometry import Polygon

from pipeline import check_prototype, etl_pipeline, find_common_year_month, join_by_stationidx, load_data, transform, transform_data1, transform_data2, transform_data3, transform_data4, transform_data5, typing_filter, transform_data6, transform_data7
from unittest.mock import patch


def assert_dataframe(df1: pd.DataFrame, df2: pd.DataFrame):

    # Reset index to ignore index during comparison
    df1 = df1.reset_index(drop=True)
    df2 = df2.reset_index(drop=True)

    # Sort the columns
    df1 = df1[sorted(df1.columns)]
    df2 = df2[sorted(df2.columns)]

    # Compare the DataFrames
    pd.testing.assert_frame_equal(df1, df2)


def test_typing_filter():

    prototype = {
        'A': 'int64',
        'B': 'float64',
        'C': 'string'
    }

    df = pd.DataFrame({
        'A': [1, '123', 33, 4, 5, 6.3],
        'B': [1, 2.1, 3, 'asd', 5, 6],
        'C': ['x', 'x', 3, '456', 7, 'x']
    })

    result = typing_filter(df, prototype)
    expected = pd.DataFrame({
        'A': [1, 123, 33, 5],
        'B': [1.0, 2.1, 3.0, 5.0],
        'C': ['x', 'x', '3', '7']
    })

    expected['A'] = expected['A'].astype('int64')
    expected['B'] = expected['B'].astype('float64')
    expected['C'] = expected['C'].astype('string')

    assert_dataframe(result, expected)

    df = pd.DataFrame({
        'A': [1, 2, 3, 4, 5, 6],
        'B': [1.0, 2.3, 3, 4, 5, 6],
        'C': ['x', 'x', 'x', 'x', 'x', 'x']
    })

    expected = pd.DataFrame({
        'A': [1, 2, 3, 4, 5, 6],
        'B': [1.0, 2.3, 3, 4, 5, 6],
        'C': ['x', 'x', 'x', 'x', 'x', 'x']
    })

    expected['A'] = expected['A'].astype('int64')
    expected['B'] = expected['B'].astype('float64')
    expected['C'] = expected['C'].astype('string')

    result = typing_filter(df, prototype)

    assert_dataframe(result, expected)


def test_check_prototype():

    prototype = {'A': 'string', 'B': 'int64', 'C': 'float64'}
    data_frame1 = pd.DataFrame({
        'A': ['1', '2'],
        'B': [1, 2],
    })

    with pytest.raises(TypeError, match=r'^Expected columns:.*? but actually:.*?, difference:.*?$'):
        check_prototype(prototype, data_frame1)

    data_frame2 = pd.DataFrame({
        'A': ['1', '2'],
        'B': [1, 2],
        'C': [2.0, 1.0],
        'X': [1, 2]
    })

    check_prototype(prototype, data_frame2)


class TestTransform1():
    def test_selection(self):

        data = pd.DataFrame(
            {'country': ['World', 'World', 'World', 'x', 'x'],
             'co2': [np.NAN, 1, 2, 3, 4],
             'co2_growth_prct': [2, np.NaN, 0.2, 0.3, 0.3],
             'year': [1901, 1900, 1902, 1903, 1904],
             'redundant column': [1, 1, 1, 1, 1]
             })

        result = transform_data1(df=data)
        expected = pd.DataFrame(
            {
                'year': [1900, 1902],
                'co2': [1.0, 2.0],
                'co2_growth_prct': [0, 0.2],
            })

        assert_dataframe(result, expected)

    def test_initial_growth(self):
        data1 = pd.DataFrame(
            {'country': ['World', 'World'],
             'co2': [1, 2,],
             'co2_growth_prct': [0.2, 0.3],
             'year': [1902, 1903],
             })

        data2 = pd.DataFrame(
            {'country': ['World', 'World'],
             'co2': [1, 2,],
             'co2_growth_prct': [np.NAN, 0.3],
             'year': [1902, 1903],
             })

        result1 = transform_data1(df=data1)
        result2 = transform_data1(df=data2)

        expected1 = pd.DataFrame(
            {
                'co2': [1.0, 2.0,],
                'co2_growth_prct': [0.2, 0.3],
                'year': [1902, 1903],
            })

        expected2 = pd.DataFrame(
            {
                'co2': [1.0, 2.0,],
                'co2_growth_prct': [0, 0.3],
                'year': [1902, 1903],
            })

        assert_dataframe(result1, expected1)
        assert_dataframe(result2, expected2)


def test_transform2():

    df_list = [pd.read_csv('./test/test_transform2_in1.csv', delimiter=';'),
               pd.read_csv('./test/test_transform2_in2.csv', delimiter=';')]
    result = transform_data2(df_list)
    expected = pd.read_csv('./test/test_transform2_out.csv')

    assert_dataframe(result, expected)


def test_transform3():
    df_list = [pd.read_csv('./test/test_transform3_in1.csv', delimiter=';'),
               pd.read_csv('./test/test_transform3_in2.csv', delimiter=';')]
    result = transform_data3(df_list)
    expected = pd.read_csv('./test/test_transform3_out.csv')

    assert_dataframe(result, expected)


def test_transform4():
    df_list = [pd.read_csv('./test/test_transform4_in1.csv', delimiter=';'),
               pd.read_csv('./test/test_transform4_in2.csv', delimiter=';')]
    result = transform_data4(df_list)
    expected = pd.read_csv('./test/test_transform4_out.csv')

    assert_dataframe(result, expected)


def test_transform5():
    df = pd.DataFrame({
        'Stationsindex': [44, 123, 456, 222, None],
        'Name': ['FAU', 'Nurnberg station', 'Kräme', 'Heidelberg', 'NULL cell'],
        'Bundesland': ['Bayern', 'Bayern', 'Thüringen', 'Baden-Württemberg', 'NULL cell'],
        'Redundant colum': ['1', '2', '23', 'test', 'NULL cell']
    },)

    result = transform_data5(df)
    expected = pd.DataFrame({
        'Stationsindex': [44, 123, 456, 222],
        'Name': ['FAU', 'Nurnberg station', 'Kräme', 'Heidelberg'],
        'Bundesland': ['Bayern', 'Bayern', 'Thueringen', 'Baden-Wuerttemberg'],
    })

    expected['Stationsindex'] = expected['Stationsindex'].astype('int64')
    expected['Name'] = expected['Name'].astype('string')
    expected['Bundesland'] = expected['Bundesland'].astype('string')

    assert_dataframe(result, expected)


def test_transform6():
    df = pd.DataFrame({
        'plz': ['1001', '1002', '1003'],
        'note': ['test1', 'test2', 'test3'],
        'einwohner': [100, 200, 300],
        'qkm': [1.1, 1.2, 1.3],
        'geometry': [Polygon([(30, 10), (40, 40), (20, 40), (10, 20), (30, 10)]),
                     Polygon([(15, 5), (25, 25), (5, 25), (5, 5), (15, 5)]),
                     Polygon([(15, 5), (25, 25), (5, 25), (5, 5), (30, 5)])]})

    df['plz'] = df['plz'].astype('string')

    result = transform_data6(df)

    expected = df[['plz', 'geometry']]

    assert_dataframe(result, expected)


def test_transform7():

    df = pd.DataFrame({
        'ags': [1, 2, 3],
        'ort': ['Nurnberg', 'Erlangen', 'Forchheim'],
        'plz': ['91052', '91054', '91056'],
        'landkreis': ['Deutschland', 'Deutschland', 'Deutschland'],
        'bundesland': ['Bayern', 'Bayern', 'Bayern'],
    })

    df['plz'] = df['plz'].astype('string')
    df['bundesland'] = df['bundesland'].astype('string')
    df['ort'] = df['ort'].astype('string')

    result = transform_data7(df)

    expected = df[['ort', 'plz', 'bundesland']]
    expected.rename(columns={'bundesland': 'Bundesland'}, inplace=True)

    assert_dataframe(result, expected)


def joint_by_plz():

    df5 = pd.DataFrame({
        'plz': ['90402', '91301', '91052'],
        'geometry': [Polygon([(30, 10), (40, 40), (20, 40), (10, 20), (30, 10)]),
                     Polygon([(15, 5), (25, 25), (5, 25), (5, 5), (15, 5)]),
                     Polygon([(15, 5), (25, 25), (5, 25), (5, 5), (30, 5)])]
    })

    df5['plz'] = df5['plz'].astype('string')

    df6 = pd.DataFrame({
        'plz': ['90402', '91301', '91052'],
        'ort': ['Nurnberg', 'Erlangen', 'Forchheim'],
        'Bundesland': ['Bayern', 'Bayern', 'Bayern']
    })

    df6['plz'] = df6['plz'].astype('string')
    df6['Bundesland'] = df6['Bundesland'].astype('string')
    df6['ort'] = df6['ort'].astype('string')

    result = joint_by_plz(df5, df6)

    excepted = pd.DataFrame({
        'plz': ['90402', '91301', '91052'],
        'geometry': [Polygon([(30, 10), (40, 40), (20, 40), (10, 20), (30, 10)]),
                     Polygon([(15, 5), (25, 25), (5, 25), (5, 5), (15, 5)]),
                     Polygon([(15, 5), (25, 25), (5, 25), (5, 5), (30, 5)])],
        'ort': ['Nurnberg', 'Erlangen', 'Forchheim'],
        'Bundesland': ['Bayern', 'Bayern', 'Bayern']
    })

    excepted['plz'] = excepted['plz'].astype('string')
    excepted['Bundesland'] = excepted['Bundesland'].astype('string')
    excepted['ort'] = excepted['ort'].astype('string')

    assert_dataframe(result, excepted)


def test_join_by_stationidx():
    df4 = pd.read_csv('./test/test_join_stationindex_in1.csv')
    df5 = pd.read_csv('./test/test_join_stationindex_in2.csv')

    result = join_by_stationidx(df4, df5)

    expected = pd.read_csv('./test/test_join_stationindex_out.csv')

    assert_dataframe(result, expected)


def test_find_common_year_month():

    df2 = pd.DataFrame({
        'year': [1901, 1901, 1902, 1902, 1903, 1903, 1904, 1904],
        'month': [1, 2, 1, 2, 1, 2, 5, 6]
    })

    df3 = pd.DataFrame({
        'year': [1901, 1901, 1902, 1902],
        'month': [1, 2, 1, 2]
    })

    df45 = pd.DataFrame({
        'year': [1901, 1901, 1904, 1904],
        'month': [1, 2, 5, 6]
    })

    result = find_common_year_month(df2, df3, df45)
    assert result == {(1901, 1), (1901, 2)}


def test_transform():

    df1 = pd.read_csv('./test/test_transform/df1.csv')
    df2_list = [pd.read_csv('./test/test_transform/df2_1.csv'),
                pd.read_csv('./test/test_transform/df2_2.csv')]
    df3_list = [pd.read_csv('./test/test_transform/df3_1.csv'),
                pd.read_csv('./test/test_transform/df3_2.csv')]
    df4_list = [pd.read_csv('./test/test_transform/df4_1.csv'),
                pd.read_csv('./test/test_transform/df4_2.csv')]
    df5 = pd.read_csv('./test/test_transform/df5.csv')
    df6 = gpd.GeoDataFrame({
        'plz': ['90402', '91052', '91301'],
        'note': ['test1', 'test2', 'test3'],
        'einwohner': [100, 200, 300],
        'qkm': [1.1, 1.2, 1.3],
        'geometry': [Polygon([(30, 10), (40, 40), (20, 40), (10, 20), (30, 10)]),
                     Polygon([(15, 5), (25, 25), (5, 25), (5, 5), (15, 5)]),
                     Polygon([(15, 5), (25, 25), (5, 25), (5, 5), (30, 5)])]
    })

    df6['plz'] = df6['plz'].astype('string')

    df7 = pd.DataFrame({
        'ags': [1, 2, 3],
        'ort': ['Nurnberg', 'Erlangen', 'Forchheim'],
        'plz': ['90402', '91052', '91301'],
        'landkreis': ['Deutschland', 'Deutschland', 'Deutschland'],
        'bundesland': ['Bayern', 'Bayern', 'Bayern'],
    })

    df7['plz'] = df7['plz'].astype('string')
    df7['bundesland'] = df7['bundesland'].astype('string')
    df7['ort'] = df7['ort'].astype('string')

    result1, result2, result3, result45, result67 = transform(
        df1, df2_list, df3_list, df4_list, df5, df6, df7)

    expected1 = pd.read_csv('./test/test_transform/out1.csv')
    expected2 = pd.read_csv('./test/test_transform/out2.csv')
    expected3 = pd.read_csv('./test/test_transform/out3.csv')
    expected45 = pd.read_csv('./test/test_transform/out45.csv', dtype={
        'Name': 'string',
        'Bundesland': 'string'
    })

    expected67 = gpd.GeoDataFrame({
        'plz': ['90402', '91052', '91301'],
        'geometry': [Polygon([(30, 10), (40, 40), (20, 40), (10, 20), (30, 10)]),
                     Polygon([(15, 5), (25, 25), (5, 25), (5, 5), (15, 5)]),
                     Polygon([(15, 5), (25, 25), (5, 25), (5, 5), (30, 5)])],
        'ort': ['Nurnberg', 'Erlangen', 'Forchheim'],
        'Bundesland': ['Bayern', 'Bayern', 'Bayern']
    })

    expected67['plz'] = expected67['plz'].astype('string')
    expected67['Bundesland'] = expected67['Bundesland'].astype('string')
    expected67['ort'] = expected67['ort'].astype('string')

    assert_dataframe(result1, expected1)
    assert_dataframe(result2, expected2)
    assert_dataframe(result3, expected3)
    assert_dataframe(result45, expected45)
    assert_dataframe(result67, expected67)


def test_load():

    df1 = pd.read_csv('./test/test_transform/out1.csv')
    df2 = pd.read_csv('./test/test_transform/out2.csv')
    df3 = pd.read_csv('./test/test_transform/out3.csv')
    df45 = pd.read_csv('./test/test_transform/out45.csv', dtype={
        'Name': 'string',
        'Bundesland': 'string'
    })

    df67 = gpd.GeoDataFrame({
        'plz': ['90402', '91301', '91052'],
        'geometry': [Polygon([(30, 10), (40, 40), (20, 40), (10, 20), (30, 10)]),
                     Polygon([(15, 5), (25, 25), (5, 25), (5, 5), (15, 5)]),
                     Polygon([(15, 5), (25, 25), (5, 25), (5, 5), (30, 5)])],
        'ort': ['Nurnberg', 'Erlangen', 'Forchheim'],
        'Bundesland': ['Bayern', 'Bayern', 'Bayern']
    })

    df67['plz'] = df67['plz'].astype('string')
    df67['Bundesland'] = df67['Bundesland'].astype('string')
    df67['ort'] = df67['ort'].astype('string')

    db_file = './test/test.db'
    geo_json_path = './test/test.geojson'

    load_data(db_file, geo_json_path, df1, df2, df3, df45, df67)
    assert os.path.exists(db_file)

    with sqlite3.connect(db_file) as conn:

        query1 = 'select * from co2'
        query2 = 'select * from temperature'
        query3 = 'select * from precipitation'
        query45 = 'select * from soil'

        expected1 = pd.read_sql_query(query1, conn)
        expected2 = pd.read_sql_query(query2, conn)
        expected3 = pd.read_sql_query(query3, conn)
        expected45 = pd.read_sql_query(query45, conn, dtype={
            'Name': 'string',
            'Bundesland': 'string'
        })

        expected67 = gpd.read_file(geo_json_path)
        expected67['plz'] = expected67['plz'].astype('string')
        expected67['Bundesland'] = expected67['Bundesland'].astype('string')
        expected67['ort'] = expected67['ort'].astype('string')

        assert_dataframe(df1, expected1)
        assert_dataframe(df2, expected2)
        assert_dataframe(df3, expected3)
        assert_dataframe(df45, expected45)
        assert_dataframe(df67, expected67)

        os.remove(db_file)


class TestPipeline():

    @staticmethod
    def mock_read(save_position1, save_position2, save_position3, save_position4, save_position5, save_position6, save_position7):

        df1 = pd.read_csv('./test/test_transform/df1.csv')
        df2_list = [pd.read_csv('./test/test_transform/df2_1.csv'),
                    pd.read_csv('./test/test_transform/df2_2.csv')]
        df3_list = [pd.read_csv('./test/test_transform/df3_1.csv'),
                    pd.read_csv('./test/test_transform/df3_2.csv')]
        df4_list = [pd.read_csv('./test/test_transform/df4_1.csv'),
                    pd.read_csv('./test/test_transform/df4_2.csv')]
        df5 = pd.read_csv('./test/test_transform/df5.csv')

        df6 = pd.DataFrame({
            'plz': ['90402', '91052', '91301'],
            'note': ['test1', 'test2', 'test3'],
            'einwohner': [100, 200, 300],
            'qkm': [1.1, 1.2, 1.3],
            'geometry': [Polygon([(30, 10), (40, 40), (20, 40), (10, 20), (30, 10)]),
                         Polygon([(15, 5), (25, 25), (5, 25), (5, 5), (15, 5)]),
                         Polygon([(15, 5), (25, 25), (5, 25), (5, 5), (30, 5)])],
        })

        df6['plz'] = df6['plz'].astype('string')

        df7 = pd.DataFrame({
            'ags': [1, 2, 3],
            'ort': ['Nurnberg', 'Erlangen', 'Forchheim'],
            'plz': ['90402', '91052', '91301'],
            'landkreis': ['Deutschland', 'Deutschland', 'Deutschland'],
            'bundesland': ['Bayern', 'Bayern', 'Bayern'],
        })

        df7['plz'] = df7['plz'].astype('string')
        df7['bundesland'] = df7['bundesland'].astype('string')
        df7['ort'] = df7['ort'].astype('string')

        return df1, df2_list, df3_list, df4_list, df5, df6, df7

    @staticmethod
    def mock_download(link, save_position):
        return

    @staticmethod
    def mock_load(db_file, geo_json_path, df1, df2, df3, df45, df67):
        return

    def test_pipeline(self):

        data1_path = '../data/download/data1/data1.csv'
        data2_directory = '../data/download/data2'
        data3_directory = '../data/download/data3'
        data4_directory = '../data/download/data4'
        data5_path = '../data/download/data5/data5.csv'
        data6_path = '../data/download/data6/data6.zip'
        data7_path = '../data/download/data7/data7.csv'

        link1 = 'https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.csv'
        link2 = 'https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean'
        link3 = 'https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/precipitation'
        link4 = 'https://opendata.dwd.de/climate_environment/CDC/derived_germany/soil/monthly/historical'
        link5 = 'https://opendata.dwd.de/climate_environment/CDC/derived_germany/soil/monthly/historical/derived_germany_soil_monthly_historical_stations_list.txt'
        link6 = 'https://downloads.suche-postleitzahl.org/v2/public/plz-5stellig.shp.zip'
        link7 = 'https://downloads.suche-postleitzahl.org/v2/public/zuordnung_plz_ort.csv'
        db_file = '../data/data.sql'
        geo_json_path = '../data/place.geojson'

        with patch("pipeline.download_data1", side_effect=self.mock_download) as mock_download1, patch("pipeline.download_data2", side_effect=self.mock_download) as mock_download2, patch("pipeline.download_data3", side_effect=self.mock_download) as mock_download3, patch("pipeline.download_data4", side_effect=self.mock_download) as mock_download4, patch("pipeline.download_data5", side_effect=self.mock_download) as mock_download5, patch("pipeline.download_data6", side_effect=self.mock_download) as mock_download6, patch("pipeline.download_data7", side_effect=self.mock_download) as mock_download7, patch("pipeline.read_csv_from_saved_positions", side_effect=self.mock_read) as mock_read, patch("pipeline.load_data", side_effect=self.mock_load) as mock_load:

            etl_pipeline(link1, link2, link3, link4, link5, link6, link7, data1_path,
                         data2_directory, data3_directory, data4_directory, data5_path, data6_path, data7_path, db_file, geo_json_path)

            mock_download1.assert_called_once_with(link1, data1_path)
            mock_download2.assert_called_once_with(link2, data2_directory)
            mock_download3.assert_called_once_with(link3, data3_directory)
            mock_download4.assert_called_once_with(link4, data4_directory)
            mock_download5.assert_called_once_with(link5, data5_path)
            mock_download7.assert_called_once_with(link7, data7_path)
            mock_download6.assert_called_once_with(link6, data6_path)

            mock_read.assert_called_once_with(
                data1_path, data2_directory, data3_directory, data4_directory, data5_path, data6_path, data7_path)
            mock_load.assert_called_once()


def test_system():

    data1_path = '../data/download/data1/data1.csv'
    data2_directory = '../data/download/data2'
    data3_directory = '../data/download/data3'
    data4_directory = '../data/download/data4'
    data5_path = '../data/download/data5/data5.csv'
    data6_path = '../data/download/data6/data6.zip'
    data7_path = '../data/download/data7/data7.csv'

    link1 = 'https://nyc3.digitaloceanspaces.com/owid-public/data/co2/owid-co2-data.csv'
    link2 = 'https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean'
    link3 = 'https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/precipitation'
    link4 = 'https://opendata.dwd.de/climate_environment/CDC/derived_germany/soil/monthly/historical'
    link5 = 'https://opendata.dwd.de/climate_environment/CDC/derived_germany/soil/monthly/historical/derived_germany_soil_monthly_historical_stations_list.txt'
    link6 = 'https://downloads.suche-postleitzahl.org/v2/public/plz-5stellig.shp.zip'
    link7 = 'https://downloads.suche-postleitzahl.org/v2/public/zuordnung_plz_ort.csv'

    db_file = '../data/data.sql'
    geo_json_path = '../data/place.geojson'

    if os.path.exists('../data/download/'):
        shutil.rmtree('../data/download/')

    if os.path.exists(db_file):
        os.remove(db_file)
    if os.path.exists(geo_json_path):
        os.remove(geo_json_path)

    etl_pipeline(link1, link2, link3, link4, link5, link6, link7, data1_path,
                 data2_directory, data3_directory, data4_directory, data5_path, data6_path, data7_path, db_file, geo_json_path)

    assert Path(data1_path).is_file
    assert Path(data2_directory).is_dir
    assert Path(data3_directory).is_dir
    assert Path(data4_directory).is_dir
    assert Path(data5_path).is_file
    assert Path(data6_path).is_file
    assert Path(data7_path).is_file

    assert os.path.exists(db_file)
    assert os.path.exists(geo_json_path)
