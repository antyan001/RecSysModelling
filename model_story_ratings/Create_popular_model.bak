#!/bin/env python3


import os, sys

recsys_cf = os.environ.get("RECSYS_STORY")
sys.path.append(recsys_cf)
os.chdir(recsys_cf)

from lib.tools import *
from datetime import datetime, timedelta
from urllib.parse import urlparse
from pyspark.sql.types import *

INTERVAL_DAYPART = 28



class CreatePopularModel():


    ################################## init ##################################


    def __init__(self):
        # self.REFDATE = "2019-09-01"
        self.script_name = "Create_popular_story_model"
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')
        self.interval_daypart = INTERVAL_DAYPART

        self.output_scheme = SBX_TEAM_DIGITCAMP
        self.output_table = RECSYS_STORY_CLICKSTREAM

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
        # Load clickstream
        start_d = (datetime.now() - timedelta(days=INTERVAL_DAYPART)).strftime('%Y-%m-%d')
        end_d = (datetime.now()).strftime('%Y-%m-%d')

        sql_query_ctl = """
        SELECT DISTINCT ctl_loading
        FROM {scheme}.{ctl_table}
        WHERE 1=1
        AND (min_sessiondate >= '{start_date}'
            OR max_sessiondate >= '{start_date}')
        AND (min_sessiondate < '{end_date}'
            OR max_sessiondate < '{end_date}')
        """.format(scheme=SBX_TEAM_DIGITCAMP,
                  ctl_table=GA_VISIT_CTL_DATE_MAP,
                  start_date=start_d,
                  end_date=end_d)

        ctl_loading_list = self.hive.sql(sql_query_ctl).toPandas()["ctl_loading"]
        min_ctl = ctl_loading_list.min()
        max_ctl = ctl_loading_list.max()

        visit_query = """
        SELECT sbbolUserId,
            sessionDate,
            sessionStartTime,
            visitNumber,
            hitTime,
            hitNumber,
            hitPagePath,
            hitType,
            eventCategory,
            eventAction,
            eventLabel
        FROM {visit_scheme}.{visit_tb_name}
        WHERE 1=1
        AND ctl_loading >= {min_ctl_loading}
        AND ctl_loading < {max_ctl_loading}
        AND sessiondate >= '{start_date}'
        AND hitPageHostName like 'sbi.sberbank.ru'
        """.format(visit_scheme=DEFAULT,
                   visit_tb_name=EXTERNAL_GOOGLE_ANALYTICS,
                   start_date=start_d,
                   min_ctl_loading=min_ctl,
                   max_ctl_loading=max_ctl)

        sbbol_clickstream = self.hive.sql(visit_query)
        # Add product
        pattern1 = 'product\: [^]]*]{1}'
        pattern2 = 'product\: [^,]*,{1}'
        clickstream_product = sbbol_clickstream.withColumn('product',f.regexp_extract("eventLabel", pattern1, 0))\
                                               .withColumn('product',
                                                            f.when(f.col("product").like("%,%"),
                                                            f.regexp_extract("eventLabel", pattern2, 0))\
                                               .otherwise(f.col("product")))
        clickstream_product = clickstream_product.withColumn("product",
                                                             f.col("product").substr(f.lit(10), f.length("product") - 10))
        #story_shows
        sbbol_story_show = clickstream_product.filter("eventCategory like '%[operations]: offers%'")\
                                              .filter("eventAction like '%impression-story%'")\
                                              .filter("eventLabel like '%[placement: main[main_feed]]%'")
        
        story_show_cnt = sbbol_story_show.groupBy("product").agg(f.count("sbbolUserId").alias("story_show_cnt"),
                                                                 f.countDistinct("sbbolUserId").alias("story_show_dist_cnt"))\
                                         .orderBy(f.col("story_show_cnt").desc())  
        #story_cliks
        sbbol_story_click = clickstream_product.filter("eventCategory like '%[operations]: offers%'")\
                                       .filter("eventAction like 'open card'")\
                                       .filter("eventLabel like '%[placement: main[main_feed]]%'").cache()
        
        story_click_cnt = sbbol_story_click.groupBy("product").agg(f.count("sbbolUserId").alias("story_click_cnt"),
                                                                   f.countDistinct("sbbolUserId").alias("story_click_dist_cnt"))\
                                           .orderBy(f.col("story_click_cnt").desc())     
        #story_stats                                      
        story_show_stats = story_show_cnt.join(story_click_cnt,
                                               on='product',
                                               how='left')\
                                         .withColumn("story_click_dist_cnt",
                                                     f.when(f.col("story_click_dist_cnt").isNull(), 0)\
                                                      .otherwise(f.col("story_click_dist_cnt")))\
                                         .withColumn("story_click_cnt",
                                                     f.when(f.col("story_click_cnt").isNull(), 0)\
                                                      .otherwise(f.col("story_click_cnt")))\
                                         .withColumn("perc",
                                                     f.col("story_click_dist_cnt") / f.col("story_show_dist_cnt"))\
                                         .orderBy(f.col("perc").desc())
        #popular_model
        now_time = datetime.fromtimestamp(time.time())
        popular_model_ratings = story_show_stats.filter((f.col("story_show_cnt") >= 1000) | (f.col("product").isin(MUST_HAVE_PRODUCT_LIST)))\
                                                .filter("product <> 'none'")\
                                                .select(f.col("product").alias("prod_cd_asup"),
                                                        f.col("perc").alias("ratings"))\
                                                .withColumn('load_dt', f.lit(f.current_date().cast(StringType())))

        drop_table(SBX_TEAM_DIGITCAMP, RECSYS_STORY_POPULAR_MODEL, self.hive)
        create_table_from_df(SBX_TEAM_DIGITCAMP, RECSYS_STORY_POPULAR_MODEL, popular_model_ratings, self.hive)


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
    with CreatePopularModel() as create_popular_model:
        create_popular_model.run()
