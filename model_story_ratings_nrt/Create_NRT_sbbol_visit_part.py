#!/bin/env python3


import os, sys

recsys_cf = os.environ.get("RECSYS_STORY")
sys.path.append(recsys_cf)
os.chdir(recsys_cf)

from lib.tools import *
from datetime import datetime, timedelta
from urllib.parse import urlparse
from pyspark.sql.types import *


def get_value(col, key):
    if isinstance(col, str):
        js = json.loads(col)
        for item in js:
            if item['key'] == key:
                return item['value']
    elif isinstance(col, list):
        for i in range(len(col)):
            if col[i].key == key:
                return col[i].value

    
@udf(StringType())
def get_cid(col):
    try:
        return ".".join(get_value(col, "_ga").split(".")[-2:])
    except:
        return None


@udf(StringType())
def get_sbbolUserId(col):
    return get_value(col, "sbbolUserId") 


@udf(StringType())
def get_sbbolorguid(col):
    return get_value(col, "sbbolOrgGuid")


@udf(StringType())
def get_hitPageHostName(col):
    try:
        return get_value(col, "hitPageHostName").split(":")[1][2:]
    except:
        return None
    
    
@udf(StringType())
def get_hitPagePath(col):
    return get_value(col, "hitPagePath")        
    

@udf(IntegerType())
def get_timestamp(col):
    dt = datetime.strptime(col.split(".")[0], "%Y-%m-%dT%H:%M:%S")
    return int(dt.timestamp())

 
@udf(StringType())
def get_hitTime(col):
    return get_value(col, "sbbolMetricTimeSinceSessionStart")

    
@udf(StringType())
def get_eventLabel(col):
    return get_value(col, "eventLabel")


@udf(StringType())
def get_hitPageTitle(col):
    return get_value(col, "eventPageName")

@udf(StringType())
def get_sessionId(col):
    try:
        return col.split(":")[1]
    except:
        return None




class SbbolTransformClickstreamNrt():


    ################################## init ##################################


    def __init__(self):
        # self.REFDATE = "2019-09-01"
        self.script_name = "TRANSFORM_CLICKSTREAM_SBBOL_NRT"
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')
        self.interval_daypart = 14

        self.input_scheme = "prx_nrt_sbbol_clickstream_nrt_sbbol_clickstream"
        self.input_table = "sbbol_events"

        self.output_scheme = SBX_TEAM_DIGITCAMP
        self.output_table = "RECSYS_STORY_CLICKSTREAM_NRT"

        self.init_logger()
        log("# __init__ : begin", self.logger)
        self.start_spark()


    ############################## Spark ##############################


    @class_method_logger
    def start_spark(self):
        self.sp = spark(schema=SBX_TEAM_DIGITCAMP,
                        process_label=self.script_name,
                        replication_num=1,
                        kerberos_auth=False,
                        numofcores=12,
                        numofinstances=12)

        self.hive = self.sp.sql


    ################################## run ##################################


    @class_method_logger
    def run(self):
  
        visit = self.load_nrt_visit_part()
        visit_ts = self.visit_col_prepr(visit)
        visit_inn_cid = self.match_inn_cid_sbboluser_id(visit_ts)
        clickstream_filter = self.clickstream_filter_action(visit_inn_cid)
        clickstream_product = self.clickstream_parse_products(clickstream_filter)
        clickstream_final = self.create_final_table(clickstream_product)
        self.save_table(clickstream_final)


    ################################## load_visit ##################################


    @class_method_logger
    def _get_begin_date(self, interval: int):
        self.begin_date = (datetime.now() - timedelta(days=interval)).strftime('%Y-%m-%d')
#         self.begin_date = '2021-08-01'


    @class_method_logger
    def load_nrt_visit_part(self):
        self._get_begin_date(self.interval_daypart)

        visit_query = """
                SELECT *
                FROM {visit_scheme}.{visit_tb_name}
                WHERE 1=1 
                AND timestampcolumn >= '{begin_date}'
        """.format(visit_scheme=self.input_scheme,
                   visit_tb_name=self.input_table,
                   begin_date=self.begin_date)

        nrt_clickstream = self.hive.sql(visit_query).cache()

        col_list = [
            "cid",
            "sbbolUserId",
            "sbbolorgguid",
            "hitPagePath",
            "hitPageTitle",
            "sessionId",
            "timestamp",
            "sessionDate",
            "hitType",
            "hitTime",
            "eventCategory",
            "eventAction",
            "eventLabel",
            "hitPageHostName"
        ]

        nrt_visit_part = nrt_clickstream.withColumn("cid", get_cid('profile_cookie'))\
                                        .withColumn("sbbolUserId", get_sbbolUserId('data_properties'))\
                                        .withColumn("sbbolorgguid", get_sbbolorguid('data_properties'))\
                                        .withColumn("hitPageHostName", get_hitPageHostName('data_properties'))\
                                        .withColumn("hitPagePath", get_hitPagePath('data_properties'))\
                                        .withColumn("sessionId", get_sessionId(f.col("profile_sessionid")))\
                                        .withColumn("timestamp", get_timestamp("data_timeStamp"))\
                                        .withColumn("sessionDate", f.col("timestampcolumn"))\
                                        .withColumn("hitType", f.when(f.col("data_eventaction") == f.lit("event"), "EVENT")\
                                                                .when(f.col("data_eventaction") == f.lit("pageview"), "PAGE")\
                                                                .otherwise("EXCEPTION"))\
                                        .withColumn("hitTime", get_hitTime("data_properties"))\
                                        .withColumn("eventCategory", f.col("data_eventcategory"))\
                                        .withColumn("eventAction", f.col("data_eventtype"))\
                                        .withColumn("eventLabel", get_eventLabel('data_properties'))\
                                        .withColumn("hitPageTitle", get_hitPageTitle("data_properties"))\
                                        .select(*col_list)\
                                        .filter(f.to_timestamp(f.col("timestamp")) >= "'{begin_date}'".format(begin_date=self.begin_date))\
                                        .filter("hitPageHostName = 'sbi.sberbank.ru'")\
                                        .cache()
                                        
        log("### Visit count sbbol nrt: " + str(nrt_visit_part.count()), self.logger)
        # log("### Visit date sbbol nrt: " + str(nrt_visit_part.select(f.min(f.col("sessionDate"))).show()) + \
        #     ' ' + str(nrt_visit_part.select(f.max(f.col("sessionDate"))).show(), self.logger)
        return nrt_visit_part


    ################################## set_preproc_event ##################################


    @staticmethod
    @udf(stypes.StringType())
    def _get_url_path(url):
        try:
            url_parse = urlparse(url)
        except Exception as e:
            print(e)
            return None

        return url_parse.path


    @class_method_logger
    def visit_col_prepr(self, visit_part):
        visit_cols = visit_part.withColumn("hitPagePath", self._get_url_path(f.col("hitPagePath")))\
                               .filter(f.col("hitPagePath") != "/" )

        return visit_cols

    ################################## match_inn_cid_sbboluserid ##################################


    @class_method_logger
    def load_customer_sbbol(self):
        return load_table(SBX_TEAM_DIGITCAMP, GA_MA_CUSTOMER_SBBOL, self.hive).cache()


    @class_method_logger
    def match_inn_cid_sbboluser_id(self, visit_ts):
        customer_sbbol = self.load_customer_sbbol()

        visit_ts = customer_sbbol.select("REPLICATIONGUID", f.col("CU_INN").alias("inn"))\
                         .join(visit_ts,
                               customer_sbbol.REPLICATIONGUID == visit_ts.sbbolorgguid,
                               "inner")\
                         .drop("REPLICATIONGUID", "sbbolorgguid")\
                         .distinct()
        visit_ts = visit_ts.filter(~f.isnull("inn"))
        return visit_ts

    ################################## filter_actions ##################################


    @class_method_logger
    def clickstream_filter_action(self, visit_ts):
        STORY_CLICK = "hitType = 'EVENT' AND eventCategory = '[operations]: offers' AND eventAction  = 'open card'"
        USER_CLICK = "eventAction = 'click'"
        clickstream_filter = visit_ts.filter(STORY_CLICK + " OR " + USER_CLICK)
        clickstream_uniq = clickstream_filter.distinct().cache()
        return clickstream_uniq


    ################################## parse_products ##################################


    @class_method_logger
    def clickstream_parse_products(self, clickstream_uniq):
        pattern1 = 'product\: [^]]*]{1}'
        pattern2 = 'product\: [^,]*,{1}'
        clickstream_product = clickstream_uniq.withColumn('product',f.regexp_extract("eventLabel", pattern1, 0))\
                                              .withColumn('product',
                                                          f.when(f.col("product").like("%,%"),
                                                          f.regexp_extract("eventLabel", pattern2, 0))\
                                              .otherwise(f.col("product")))
        clickstream_product = clickstream_product.withColumn("product",
                                                             f.col("product").substr(f.lit(10), f.length("product") - 10))
        return clickstream_product


    ################################## final_table ##################################


    @class_method_logger
    def create_final_table(self, clickstream_product):
        clickstream_final = clickstream_product.select('inn',
                                               'sbbolUserId',
                                               'timestamp',
                                               'hitPagePath',
                                               "hitPageTitle",
                                               'eventCategory',
                                               'eventAction',
                                               'eventLabel',
                                               'product').distinct()
        return clickstream_final


    ################################## save_table ##################################


    @class_method_logger
    def save_table(self, clickstream_final):
        try:
            drop_table(SBX_TEAM_DIGITCAMP, self.output_table, self.hive)
            create_table_from_df(SBX_TEAM_DIGITCAMP, self.output_table, clickstream_final, self.hive)
        except:
            create_table_from_df(SBX_TEAM_DIGITCAMP, self.output_table, clickstream_final, self.hive)

    ################################## Logging ##################################


    def init_logger(self):
        self.print_log = True

        try:
            os.mkdir("logs/NRT_model/" + self.currdate)
        except:
            pass

        logging.basicConfig(filename='logs/NRT_model/{}/{}.log'.format(self.currdate, self.script_name),
                            level=logging.INFO,
                            format='%(asctime)s %(message)s')
        self.logger = logging.getLogger(__name__)

        log("="*54 + " {} ".format(self.currdate) + "="*54, self.logger)

    ############################## ContextManager ##############################


    def __enter__(self):
        return self


    @class_method_logger
    def close(self):
        try:
            self.sp.sc.stop()
        except Exception as ex:
            if "object has no attribute" in str(ex):
                log("### SparkContext was not started", self.logger)
            else:
                log("### Close exception: \n{}".format(ex), self.logger)
        finally:
            # del self.sing
            pass


    def __exit__(self, exc_type, exc_value, exc_tb):
        log("# __exit__ : begin", self.logger)
        if exc_type is not None:
            log("### Exit exception ERROR:\nexc_type:\n{}\nexc_value\n{}\nexc_tb\n{}"\
                .format(exc_type, exc_value, exc_tb), self.logger)
        self.close()
        log("# __exit__ : end", self.logger)
        log("="*120, self.logger)


if __name__ == '__main__':
    with SbbolTransformClickstreamNrt() as sbbol_transform_clickstream_nrt:
        sbbol_transform_clickstream_nrt.run()
