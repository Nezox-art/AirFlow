import requests
import pandas as pd

from clickoperator.clickhouse_operator import ClickhouseExecuteOperator
from airflow.hooks.base import BaseHook
from urllib.parse import urlencode
import os
 

# Словарь для хранения полей для каждого типа логов
log_mapping = {
    'hits': {
    'fields': 'ym:pv:watchID, ym:pv:counterID, ym:pv:date, ym:pv:dateTime, ym:pv:title, ym:pv:URL, ym:pv:referer, ym:pv:UTMCampaign, ym:pv:UTMContent, ym:pv:UTMMedium, ym:pv:UTMSource, ym:pv:UTMTerm, ym:pv:browser, ym:pv:browserMajorVersion, ym:pv:browserMinorVersion, ym:pv:browserCountry, ym:pv:browserEngine, ym:pv:browserEngineVersion1, ym:pv:browserEngineVersion2, ym:pv:browserEngineVersion3, ym:pv:browserEngineVersion4, ym:pv:browserLanguage, ym:pv:clientTimeZone, ym:pv:cookieEnabled, ym:pv:deviceCategory, ym:pv:from, ym:pv:hasGCLID, ym:pv:GCLID, ym:pv:ipAddress, ym:pv:javascriptEnabled, ym:pv:mobilePhone, ym:pv:mobilePhoneModel, ym:pv:openstatAd, ym:pv:openstatCampaign, ym:pv:openstatService, ym:pv:openstatSource, ym:pv:operatingSystem, ym:pv:operatingSystemRoot, ym:pv:physicalScreenHeight, ym:pv:physicalScreenWidth, ym:pv:regionCity, ym:pv:regionCountry, ym:pv:regionCityID, ym:pv:regionCountryID, ym:pv:screenColors, ym:pv:screenFormat, ym:pv:screenHeight, ym:pv:screenOrientation, ym:pv:screenWidth, ym:pv:windowClientHeight, ym:pv:windowClientWidth, ym:pv:lastTrafficSource, ym:pv:lastSearchEngine, ym:pv:lastSearchEngineRoot, ym:pv:lastAdvEngine, ym:pv:artificial, ym:pv:pageCharset, ym:pv:isPageView, ym:pv:link, ym:pv:download, ym:pv:notBounce, ym:pv:lastSocialNetwork, ym:pv:httpError, ym:pv:clientID, ym:pv:networkType, ym:pv:lastSocialNetworkProfile, ym:pv:goalsID, ym:pv:shareService, ym:pv:shareURL, ym:pv:shareTitle, ym:pv:iFrame, ym:pv:parsedParamsKey1, ym:pv:parsedParamsKey2, ym:pv:parsedParamsKey3, ym:pv:parsedParamsKey4, ym:pv:parsedParamsKey5, ym:pv:parsedParamsKey6, ym:pv:parsedParamsKey7, ym:pv:parsedParamsKey8, ym:pv:parsedParamsKey9, ym:pv:parsedParamsKey10', 
    'create_query':
            '''
            CREATE TABLE IF NOT EXISTS {table_name} (
            visit_id UInt64,
            counter_id UInt32,
            watch_ids Array(UInt64),
            date Date,
            date_time DateTime,
            date_time_utc DateTime,
            is_new_user UInt8,
            start_url String,
            end_url String,
            page_views UInt32,
            visit_duration UInt32,
            bounce UInt8,
            ip_address String,
            region_country String,
            region_city String,
            region_country_id UInt32,
            region_city_id UInt32,
            client_id UInt64,
            network_type String,
            goals_id Array(UInt32),
            goals_serial_number Array(UInt32),
            goals_date_time Array(DateTime),
            goals_price Array(Float64),
            goals_order Array(UInt32),
            goals_currency Array(String),
            last_traffic_source String,
            last_adv_engine String,
            last_referral_source String,
            last_search_engine_root String,
            last_search_engine String,
            last_social_network String,
            last_social_network_profile String,
            referer String,
            last_direct_click_order UInt32,
            last_direct_banner_group UInt32,
            last_direct_click_banner UInt32,
            last_direct_click_order_name String,
            last_click_banner_group_name String,
            last_direct_click_banner_name String,
            last_direct_phrase_or_cond String,
            last_direct_platform_type String,
            last_direct_platform String,
            last_direct_condition_type String,
            last_currency_id UInt32,
            from_source String,
            utm_campaign String,
            utm_content String,
            utm_medium String,
            utm_source String,
            utm_term String,
            openstat_ad String,
            openstat_campaign String,
            openstat_service String,
            openstat_source String,
            has_gclid UInt8,
            last_gclid String,
            first_gclid String,
            last_significant_gclid String,
            browser_language String,
            browser_country String,
            client_time_zone String,
            device_category String,
            mobile_phone String,
            mobile_phone_model String,
            operating_system_root String,
            operating_system String,
            browser String,
            browser_major_version String,
            browser_minor_version String,
            browser_engine String,
            browser_engine_version1 String,
            browser_engine_version2 String,
            browser_engine_version3 String,
            browser_engine_version4 String,
            cookie_enabled UInt8,
            javascript_enabled UInt8,
            screen_format String,
            screen_colors UInt32,
            screen_orientation String,
            screen_width UInt32,
            screen_height UInt32,
            physical_screen_width UInt32,
            physical_screen_height UInt32,
            window_client_width UInt32,
            window_client_height UInt32
        ) ENGINE = MergeTree
        PARTITION BY device_category
        ORDER BY date_time
    ''',
    },
    'visits': {
    'fields': 'ym:s:visitID, ym:s:counterID, ym:s:watchIDs, ym:s:date, ym:s:dateTime, ym:s:dateTimeUTC, ym:s:isNewUser, ym:s:startURL, ym:s:endURL, ym:s:pageViews, ym:s:visitDuration, ym:s:bounce, ym:s:ipAddress, ym:s:regionCountry, ym:s:regionCity, ym:s:regionCountryID, ym:s:regionCityID, ym:s:clientID, ym:s:networkType, ym:s:goalsID, ym:s:goalsSerialNumber, ym:s:goalsDateTime, ym:s:goalsPrice, ym:s:goalsOrder, ym:s:goalsCurrency, ym:s:lastTrafficSource, ym:s:lastAdvEngine, ym:s:lastReferalSource, ym:s:lastSearchEngineRoot, ym:s:lastSearchEngine, ym:s:lastSocialNetwork, ym:s:lastSocialNetworkProfile, ym:s:referer, ym:s:lastDirectClickOrder, ym:s:lastDirectBannerGroup, ym:s:lastDirectClickBanner, ym:s:lastDirectClickOrderName, ym:s:lastClickBannerGroupName, ym:s:lastDirectClickBannerName, ym:s:lastDirectPhraseOrCond, ym:s:lastDirectPlatformType, ym:s:lastDirectPlatform, ym:s:lastDirectConditionType, ym:s:lastCurrencyID, ym:s:from, ym:s:UTMCampaign, ym:s:UTMContent, ym:s:UTMMedium, ym:s:UTMSource, ym:s:UTMTerm, ym:s:openstatAd, ym:s:openstatCampaign, ym:s:openstatService, ym:s:openstatSource, ym:s:hasGCLID, ym:s:lastGCLID, ym:s:firstGCLID, ym:s:lastSignificantGCLID, ym:s:browserLanguage, ym:s:browserCountry, ym:s:clientTimeZone, ym:s:deviceCategory, ym:s:mobilePhone, ym:s:mobilePhoneModel, ym:s:operatingSystemRoot, ym:s:operatingSystem, ym:s:browser, ym:s:browserMajorVersion, ym:s:browserMinorVersion, ym:s:browserEngine, ym:s:browserEngineVersion1, ym:s:browserEngineVersion2, ym:s:browserEngineVersion3, ym:s:browserEngineVersion4, ym:s:cookieEnabled, ym:s:javascriptEnabled, ym:s:screenFormat, ym:s:screenColors, ym:s:screenOrientation, ym:s:screenWidth, ym:s:screenHeight, ym:s:physicalScreenWidth, ym:s:physicalScreenHeight, ym:s:windowClientWidth, ym:s:windowClientHeight',
    'create_query': 
        '''
            CREATE TABLE IF NOT EXISTS {table_name} (
            watch_id UInt64,
            counter_id UInt32,
            date Date,
            date_time DateTime,
            title String,
            url String,
            referer String,
            utm_campaign String,
            utm_content String,
            utm_medium String,
            utm_source String,
            utm_term String,
            browser String,
            browser_major_version String,
            browser_minor_version String,
            browser_country String,
            browser_engine String,
            browser_engine_version1 String,
            browser_engine_version2 String,
            browser_engine_version3 String,
            browser_engine_version4 String,
            browser_language String,
            client_time_zone String,
            cookie_enabled UInt8,
            device_category String,
            from_source String,
            has_gclid UInt8,
            gclid String,
            ip_address String,
            javascript_enabled UInt8,
            mobile_phone String,
            mobile_phone_model String,
            openstat_ad String,
            openstat_campaign String,
            openstat_service String,
            openstat_source String,
            operating_system String,
            operating_system_root String,
            physical_screen_height UInt32,
            physical_screen_width UInt32,
            region_city String,
            region_country String,
            region_city_id UInt32,
            region_country_id UInt32,
            screen_colors UInt32,
            screen_format String,
            screen_height UInt32,
            screen_orientation String,
            screen_width UInt32,
            window_client_height UInt32,
            window_client_width UInt32,
            last_traffic_source String,
            last_search_engine String,
            last_search_engine_root String,
            last_adv_engine String,
            artificial UInt8,
            page_charset String,
            is_page_view UInt8,
            link String,
            download UInt8,
            not_bounce UInt8,
            last_social_network String,
            http_error UInt8,
            client_id UInt64,
            network_type String,
            last_social_network_profile String,
            goals_id Array(UInt32),
            share_service String,
            share_url String,
            share_title String,
            iframe UInt8,
            parsed_params_key1 String,
            parsed_params_key2 String,
            parsed_params_key3 String,
            parsed_params_key4 String,
            parsed_params_key5 String,
            parsed_params_key6 String,
            parsed_params_key7 String,
            parsed_params_key8 String,
            parsed_params_key9 String,
            parsed_params_key10 String
        ) ENGINE = MergeTree
        PARTITION BY device_category
        ORDER BY date_time
    ''',
    } 
}


insert_data_query = 'INSERT INTO {table_name} VALUES'

# Функция для создания запроса с динамическими полями
def create_log_request(yandex_metrika_token, yandex_metrika_counter_id, start_date, end_date, source):    
    # Получение нужных полей и таблиц в зависимости от типа source
    selected_fields = log_mapping[source]['fields']

    url_params = urlencode(
        [
            ('date1', start_date),
            ('date2', end_date),
            ('source', source),
            ('fields', selected_fields)
        ]
    )

    url = f'https://api-metrika.yandex.net/management/v1/counter/{yandex_metrika_counter_id}/logrequests?'\
          + url_params
    
    headers = {'Authorization': f'OAuth {yandex_metrika_token}'}


    response = requests.post(url, headers=headers)
    if response.status_code == 200:
        log_request = json.loads(response.text)['log_request']
        return log_request['request_id']
    else:
        print(r.text)
        return ''

 
# Функция для загрузки данных из Яндекс Метрики
def download_data(request_id, yandex_metrika_token, yandex_metrika_counter_id):
    url = 'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequest/{request_id}/part/{part_number}/download'
    headers = {'Authorization': f'OAuth {yandex_metrika_token}'}
    part_number = 0
    data_frames = []
 
    while True:
        response = requests.get(
            url.format(counter_id=yandex_metrika_counter_id, request_id=request_id, part_number=part_number),
            headers=headers
        )
        if response.status_code == 204:
            break
        response.raise_for_status()
        df = pd.read_csv(response.content)
        data_frames.append(df)
        part_number += 1
 
    return pd.concat(data_frames)

# Очистка таска на сервере метрики 
def clean_data(request_id, yandex_metrika_token, yandex_metrika_counter_id):
    url = 'https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequest/{request_id}/clean' \
        .format(counter_id=yandex_metrika_counter_id,
                request_id=request_id)

    headers = {'Authorization': f'OAuth {yandex_metrika_token}'}

    r = requests.post(url, headers=headers)
    if r.status_code != 200:
        print(r.text)

    api_request.status = json.loads(r.text)['log_request']['status']
    return json.loads(r.text)['log_request']

 
# Функция для загрузки данных в ClickHouse
def load_data_to_clickhouse(df, table_name, source, type_query='insert', clickhouse_connection='new_click'):
    hook = BaseHook.get_connection(clickhouse_conn_id)

    if type_query =='create':
        create_table_query = log_mapping[source]['create_query'].format(table_name=table_name)
        ClickhouseExecuteOperator(
        	conn_id = clickhouse_connection,
        	sql = create_table_query)


    # Преобразование данных в формат, пригодный для вставки в ClickHouse
    records = df.to_dict('records')

    insert_query = insert_data_query.format(table_name=table_name)
    
    # Вставка данных в таблицу ClickHouse
    ClickhouseExecuteOperator(
        	conn_id = clickhouse_connection,
        	sql = insert_query+records)


 
def download_metrica_to_clickhouse(yandex_metrika_token, yandex_metrika_counter_id, start_date, end_date, source, table_name, type_query='insert', clickhouse_connection='new_click'):
    # Создание запроса на получение данных
    print(f"Создаем запрос для {source}...")
    request_id = create_log_request(yandex_metrika_token, yandex_metrika_counter_id, start_date, end_date, source)
    
    # Загрузка данных по запросу
    print(f"Загружаем данные для {source}...")
    df = download_data(request_id, yandex_metrika_token, yandex_metrika_counter_id)
    
    # Загрузка данных в ClickHouse
    print(f"Загружаем данные в таблицу {table_name}...")
    load_data_to_clickhouse(df, table_name, source, type_query, clickhouse_connection)

    # Очистка данных в метрике
    print(f"Очищаем запрос в метрике...")
    clean_data(request_id, yandex_metrika_token, yandex_metrika_counter_id)
    
    print(f"ETL пайплайн для {source} завершен.")
