from datetime import datetime, timedelta
from pyspark.sql.types import IntegerType, LongType
import pyspark.sql.functions as f


class Recsys360CreateClickstreamSlicetNrt():
    
    def __init__(
        self,
        hive,
        input_scheme,
        input_table,
        save_flg=False,
        output_scheme=None,
        output_table=None,
        begin_date=None,
        end_date=None
        
    ):
        self.hive = hive
        
        self.input_scheme = input_scheme
        self.input_table = input_table
        self.save_flg = save_flg
        self.output_scheme = output_scheme
        self.output_table = output_table
        
        self.interval_daypart = 1       
        self.begin_date, self.end_date = self._init_date(begin_date, end_date)
        
        
    def make_slice(self):

        nrt_clickstream_sdf = self.load_nrt_clickstream()
        nrt_clickstream_parse_sdf = self.parse_fields(nrt_clickstream_sdf)
        nrt_clickstream_filter_sdf = self.filter_parse_clickstream(nrt_clickstream_parse_sdf)
        nrt_clickstream_final = self.select_rename_sdf(nrt_clickstream_filter_sdf)

        if self.save_flg:
            self.save_table(nrt_clickstream_final)

        return nrt_clickstream_final
    
    
    def load_nrt_clickstream(self):
        SQL_NRT_query = f"""
        SELECT *
        FROM {self.input_scheme}.{self.input_table}
        WHERE 1=1 
        AND timestampcolumn >= '{self.begin_date}'
        AND timestampcolumn < '{self.end_date}'
        """
        nrt_clickstream_sdf = self.hive.sql(SQL_NRT_query).cache()
        return nrt_clickstream_sdf
    

    def parse_fields(self, nrt_clickstream_sdf):
        # mapping structField
        nrt_clickstream_map = nrt_clickstream_sdf.withColumn("map_profile_cookie",
                                                             f.map_from_arrays(f.col("profile_cookie.key"), f.col("profile_cookie.value")))\
                                                 .withColumn("data_properties_map",
                                                              f.map_from_arrays(f.col("data_properties.key"), f.col("data_properties.value")))

        # get profile_cookie fields                                                     
        nrt_clickstream_profile_cookie_fields = nrt_clickstream_map\
        .withColumn("gaUserId", f.substring(f.col("map_profile_cookie")["gaUserId"], 7, 100))\
        .withColumn("_ga", f.substring(f.col("map_profile_cookie")["_ga"], 7, 100))

        # get data_properties fields
        nrt_clickstream_data_properties_fields = nrt_clickstream_profile_cookie_fields\
        .withColumn("sbbolClientId", f.col("data_properties_map")["sbbolClientId"])\
        .withColumn("ga_cid", f.col("data_properties_map")["ga_cid"])\
        .withColumn("sbbolUserId", f.col("data_properties_map")["sbbolUserId"])\
        .withColumn("sbbolOrgGuid", f.col("data_properties_map")["sbbolOrgGuid"])\
        .withColumn("hitPageHostName", f.col("data_properties_map")["hitPageHostName"])\
        .withColumn("hitPagePath", f.col("data_properties_map")["hitPagePath"])\
        .withColumn("type", f.col("data_properties_map")["Type"])\
        .withColumn("eventPageName", f.col("data_properties_map")["eventPageName"])\
        .withColumn("eventLabel", f.col("data_properties_map")["eventLabel"])\
        .withColumn("sbbolMetricTimeSinceSessionStart", f.col("data_properties_map")["sbbolMetricTimeSinceSessionStart"].cast(IntegerType()))

        # create custom fields
        nrt_clickstream_custom_fields = nrt_clickstream_data_properties_fields\
        .withColumn("profile_appid", f.substring(f.col("profile_appid"), 7, 100))\
        .withColumn("sessionId", f.split(f.col("profile_sessionid"), ':')[1].cast(LongType()))\
        .withColumn("data_timeStamp", f.to_timestamp(f.col("data_timeStamp")))\
        .withColumn("data_timeStampInt", f.col("data_timeStamp").cast(IntegerType()))\
        .withColumn("hitType",  f.when(f.col("data_eventaction") == f.lit("event"), "EVENT")\
                                 .when(f.col("data_eventaction") == f.lit("pageview"), "PAGE")\
                                 .otherwise("EXCEPTION"))\
        .withColumn("sessionDate", f.to_date("data_timeStamp"))
            
        col_list = [
            'meta_apikey',
            'profile_appid',
            'gaUserId',
            '_ga',
            'sbbolClientId',
            'ga_cid',
            'sbbolUserId',
            'sbbolOrgGuid',
            'sessionId',
            'sessionDate',
            'hitPageHostName',
            'hitPagePath',
            'hitType',
            'data_eventaction',
            'data_eventcategory',
            'eventPageName',
            'eventLabel',
            'type',
            'data_timeStamp',
            'data_timeStampInt',
            'sbbolMetricTimeSinceSessionStart',
            'timestampcolumn'
            ]
        return nrt_clickstream_custom_fields.select(*col_list)
    

    def filter_parse_clickstream(self, parse_nrt_clickstream):
        "Фильтруем по дате (по таймстемпу попадается мусор) и хосту (нас интересуют только хост сббола)"
        host_list = [
            'https://sbi.sberbank.ru',
            'https://sbi.sberbank.ru:9443'
            ]
        filer_clicstream_sdf = parse_nrt_clickstream\
        .filter(f.col("hitPageHostName").isin(host_list))\
        .filter(f"sessionDate >= '{self.begin_date}' AND sessionDate < '{self.end_date}'")
        return filer_clicstream_sdf


    def select_rename_sdf(self, parse_sdf, col_rename_dict=None):
        if col_rename_dict is None:
            self.col_rename_dict = {
               "_ga": "cid",
               "sbbolUserId": "sbbolUserId",
               "sbbolOrgGuid": "sbbolOrgGuid",
               "sessionId": "sessionId",
               "sessionDate": "sessionDate",
               "data_timeStamp": "hitTimestamp",
               "sbbolMetricTimeSinceSessionStart": "timeSinceSessionStart",
               "hitPageHostName": "hitPageHostName",
               "hitPagePath": "hitPagePath",
               "hitType": "hitType",
               "data_eventcategory": "eventCategory",
               "data_eventaction": "eventAction",
               "eventLabel": "eventLabel",
               "timestampcolumn": "timestampcolumn" 
            }
        else: self.col_rename_dict = col_rename_dict

        select_list =  [f.col(col_name).alias(self.col_rename_dict[col_name]) for col_name in self.col_rename_dict]
        return parse_sdf.select(*select_list)


    def save_table(self, sdf):
        pass    
    

    def _init_date(self, begin_date, end_date):
        if begin_date is None:
            d1 = (datetime.now() - timedelta(days=self.interval_daypart)).strftime('%Y-%m-%d')
            d2 = (datetime.now()).strftime('%Y-%m-%d')
            return d1, d2
        
        try:
            datetime.strptime(begin_date, '%Y-%m-%d')
            datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError and TypeError:
            raise ValueError("Slice begin/end dates must be 'YYYY-MM-DD' format or both None")
            
        return begin_date, end_date