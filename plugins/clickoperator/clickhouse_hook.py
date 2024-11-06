from airflow.hooks.base_hook import BaseHook
import requests as rq
import pandas as pd
import json
from datetime import datetime

class ClickHouseException(Exception):
    def __init__(self,  message, host, status,headers):
        self.message = message
        self.host = host
        self.status = status
        self.headers = headers
        super().__init__(message)
    
    def __str__(self):
        return f'Error while connecting to ClickHouse on {self.host}. Status code {self.status}, error message: {self.message}. Headers: {self.headers}.'
        

class ClickhouseHook(BaseHook):
    """
    Hook to interact with the ClickhousePlugin.
    """

    def __init__(self, conn_id):
        # your code goes here
        self.conn = self.get_connection(conn_id)
        self.login = self.conn.login
        self.password = self.conn.password
        self.host = self.conn.host
        self.port = self.conn.port
        self.schema = self.conn.schema

    def send_get_request(self, query):
        """
        Execute get request for database
        """
        query_dict = {
            'query': query
        }
        print("Sending query:")
        print(query)
        r = rq.get(self.host, params=query_dict, auth = (self.login, self.password), verify = False)
        if r.status_code == 200:
            return r.text
        else: raise ClickHouseException(r.text, self.host, r.status_code, r.headers)

    def send_post_request(self, query, data = None):
        """
        Execute get request for database
        """
        query_dict = {
            'query':query
        }

        if data:
            print("Sending query:")
            print(query)
            r = rq.post(self.host, params=query_dict, data = data, auth=(self.login, self.password), verify=False)
            if r.status_code == 200:
                return r.text
            else: raise ClickHouseException(r.text, self.host, r.status_code, r.headers)
        else:
            print("Sending query:")
            print(query)
            r = rq.post(self.host, params=query_dict, auth=(self.login, self.password), verify=False)
            if r.status_code == 200:
                return r.text
            else: raise ClickHouseException(r.text, self.host, r.status_code, r.headers)
    
    def send_post_request_enc(self, query, data = None):
        """
        Execute get request for database
        """
        query_dict = {
            'query':query
        }

        if data:
            print("Sending query:")
            print(query)
            r = rq.post(self.host, params=query_dict, data = data.encode('utf-8'), auth=(self.login, self.password), verify=False)
            if r.status_code == 200:
                return r.text
            else: raise ClickHouseException(r.text, self.host, r.status_code, r.headers)            
        else:
            print("Sending query:")
            print(query)
            r = rq.post(self.host, params=query_dict, auth=(self.login, self.password), verify=False)
            if r.status_code == 200:
                return r.text
            else: raise ClickHouseException(r.text, self.host, r.status_code, r.headers)            

    def get_json(self, query:str):
        """
        Get SQL query on Clickhouse API in JSON format
        """
        if query.lower().find("format json") > 0:
            return json.loads(self.send_get_request(query))
        else: 
            query += " format JSON"
            return json.loads(self.send_get_request(query))

    def get_dataFrame(self, query):
        """
        Get Pandas DataFrame from Clickhouse
        """
        data = self.get_json(query)
        return pd.DataFrame(data['data'])

    def get_sql(self, query):
        """
        Get SQL query on Clickhouse API in TabSeparated format
        """

        return self.send_get_request(query)

    def execute_action(self, query:str, data=None):

        """
        Execute sql command - select, create, insert, alter and so on.
        Optional parameter - data. Initially can only be parquet
        """

        if data:
            if query.lower().find("format parquet") > 0:
                return self.send_post_request(query, data)
            else: 
                query  += "format Parquet"
                return self.send_post_request(query, data)
        else:
            return self.send_post_request(query)

    def upload_dataframe(self, df:pd.DataFrame, query = None):
        """
        Upload Pandas DataFrame to clickhouse
        """
        
        import io 
        import pyarrow.parquet as pp
        f = io.BytesIO()
        data = df.to_parquet()
        datatrans = df.to_parquet(f)

        if query:
            return self.send_post_request(query,data)
        else: 
            name = self.schema + '.Upload_' + datetime.now().strftime('%Y%m%d_%H%M%S')
            fields = {
                "bool":"UInt8",
                "float":"Float32", 
                "half_float":"Float32",
                "double":"Float64",
                "date32":"Date",
                "date64":"DateTime",
                "timestamp[us]":"DateTime",
                "timestamp[us, tz=UTC]":"DateTime",
                "binary":"String",
                "string":"String",
                "list":"Array",
                "struct":"Tuple"
            }
            meta = pp.read_schema(f)
            names = ""
            for k, v in list(zip(meta.names, meta.types)):
                for key in fields:
                    if str(v) == key:
                        v = str(fields[key])
                names += str(k)
                names += ' '
                names += str(v)
                names += ' Null,'


            query =  f'create table {name} (' + names[0:-1] + ') ENGINE = Log'
            
            self.send_post_request(query)
            query = f'insert into {name} format Parquet'
            return self.send_post_request (query, data)
