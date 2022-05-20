#!/bin/env python3


import os, sys

recsys_cf = os.environ.get("RECSYS_STORY")
sys.path.append(recsys_cf)
os.chdir(recsys_cf)

from lib.tools import *
from datetime import datetime, timedelta
from urllib.parse import urlparse

INTERVAL_DAYPART = 28



class SbbolTransformClickstream():


    ################################## init ##################################


    def __init__(self):
        # self.REFDATE = "2019-09-01"
        self.script_name = "TRANSFORM_CLICKSTREAM_SBBOL"
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')
        self.interval_daypart = INTERVAL_DAYPART

        self.output_scheme = SBX_TEAM_DIGITCAMP
        self.output_table = RECSYS_STORY_CLICKSTREAM

        self.init_logger()
        log("# __init__ : begin", self.logger)
        self.start_spark()
        self.get_launch_flg()


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

    
    ############################## launch flg ##############################


    @class_method_logger
    def get_launch_flg(self):
        try:
            table_info = self.hive.sql("show create table {}.{}".format(self.output_scheme, 
                                                                        self.output_table))\
                                                                .toPandas().createtab_stmt[0]
            transient_lastDdlTime = re.search(r"'transient_lastDdlTime' = '[0-9]*'" , table_info).group()
            create_time_str = re.search(r"[0-9]{4,20}", transient_lastDdlTime).group()
            create_time_int = int(create_time_str)
            create_dt = datetime.fromtimestamp(create_time_int)
            delta = (datetime.today() - create_dt).days
            if delta <= 2:
                self.launch_flg = True
            else:
                self.launch_flg = False
        except:
            self.launch_flg = False



    ################################## run ##################################


    @class_method_logger
    def run(self):
        if self.launch_flg:
            was_launched_message = "{} was launched less then two days ago".format(self.script_name)
            print(was_launched_message)
            log(was_launched_message, self.logger)
            return None
  
    
        visit = self.load_visit_part()
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
    def _get_min_ctl(self):
        ctl_date_map = self.hive.sql("select * from {}.{}".format(SBX_TEAM_DIGITCAMP, GA_VISIT_CTL_DATE_MAP)) \
                   .filter("min_sessiondate>='{}' ".format(self.begin_date))
        self.ctl_list = ','.join([str(x) for x in ctl_date_map.toPandas()['ctl_loading']])


    @class_method_logger
    def load_visit_part(self):
        self._get_begin_date(INTERVAL_DAYPART)
        self._get_min_ctl()
        visit = self.hive.sql("""
                            select sbbolUserId,
                                sbbolorgguid,
                                hitPagePath,
                                hitPageTitle,
                                sessionId,
                                sessionStartTime,
                                sessionDate,
                                hitType,
                                hitTime,
                                eventCategory,
                                eventAction,
                                eventLabel,
                                hitPageHostName

                            from default.google_analytics_visit
                            where 1=1
                            and hitPageHostName = 'sbi.sberbank.ru'
                            and ctl_loading in({})
                            and sessiondate >= '{}'
                            """.format(self.ctl_list, self.begin_date)).cache()
        return visit


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

        visit_ts = visit_cols.withColumn("timestamp",
                                         (f.col("sessionStartTime") + f.col("hitTime") / 1000).cast(stypes.LongType())) \
                             .drop("sessionStartTime", "hitTime")

        return visit_ts

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
            drop_table(SBX_TEAM_DIGITCAMP, RECSYS_STORY_CLICKSTREAM, self.hive)
            create_table_from_df(SBX_TEAM_DIGITCAMP, RECSYS_STORY_CLICKSTREAM, clickstream_final, self.hive)
        except:
            create_table_from_df(SBX_TEAM_DIGITCAMP, RECSYS_STORY_CLICKSTREAM, clickstream_final, self.hive)

    ################################## Logging ##################################


    def init_logger(self):
        self.print_log = True

        try:
            os.mkdir("logs/" + self.currdate)
        except:
            pass

        logging.basicConfig(filename='logs/{}/{}.log'.format(self.currdate, self.script_name),
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
    with SbbolTransformClickstream() as sbbol_transform_clickstream:
        sbbol_transform_clickstream.run()
