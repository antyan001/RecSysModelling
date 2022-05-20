#!/bin/env python3


import os, sys

recsys_cf = os.environ.get("RECSYS_STORY")
sys.path.append(recsys_cf)
os.chdir(recsys_cf)

from lib.tools import *
from datetime import datetime, timedelta




class CreateItemDictSbbol():


    ################################## init ##################################


    def __init__(self):
        # self.REFDATE = "2019-09-01"
        self.script_name = "CREATEITEMDICTSBBOL"
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')

        self.input_scheme = SBX_TEAM_DIGITCAMP
        self.input_table = RECSYS_STORY_CLICKSTREAM
        self.output_scheme = SBX_TEAM_DIGITCAMP
        self.output_table = RECSYS_STORY_ITEM_DICT


        self.init_logger()
        log("# __init__ : begin", self.logger)
        self.start_spark()


    ############################## Spark ##############################


    @class_method_logger
    def start_spark(self):
        self.sp = spark(schema=self.input_scheme,
                        process_label=self.script_name,
                        replication_num=1,
                        kerberos_auth=False,
                        numofcores=11,
                        numofinstances=11)

        self.hive = self.sp.sql


    ################################## run ##################################


    @class_method_logger
    def run(self):
        clickstream = self.load_clickstream()
        visit_item_id = self.get_item_id(clickstream)
        visit_unique = self.agg_first_action(visit_item_id)
        visit_item_stats = self.make_stats(visit_unique)
        item_dict = self.create_item_dict(visit_item_stats)
        self.save_table(item_dict)




    ################################## load_clickstream ##################################

    @class_method_logger
    def load_clickstream(self):
        clickstream = self.hive.sql("SELECT * FROM {scheme}.{table}".format(scheme=self.input_scheme, table=self.input_table))
        return clickstream



    ################################## get_item_id ##################################

    @class_method_logger
    def get_item_id(self, clickstream):
        w_event = Window().orderBy("eventAction", "eventCategory", "hitPagePath")
        visit_event_id = clickstream.withColumn("event_id", f.rank().over(w_event))
        max_event_id = visit_event_id.select(f.max("event_id")).take(1)[0]["max(event_id)"]

        w_group = Window().orderBy("product")
        visit_group_event_id = visit_event_id.withColumn("prod_id", 
                                                         f.when(~f.col("product").like(""), 
                                                                max_event_id + f.rank().over(w_group)) \
                                                          .otherwise(None))

        visit_item_id = visit_group_event_id.withColumn("item_id", f.coalesce("prod_id", "event_id"))
        
        return visit_item_id


    ################################## agg_first_action ##################################

    @class_method_logger
    def agg_first_action(self, visit_item_id):
        visit_item_id = visit_item_id.select("inn",
                                     "timestamp",
                                     "hitPagePath",
                                     "eventCategory",
                                     "eventAction",
                                     "product",
                                     "event_id",
                                     "prod_id",
                                     "product",
                                     "item_id").distinct()

        cols = [f.col(col) for col in visit_item_id.columns if col != "timestamp"]
        visit_unique = visit_item_id.groupBy(*cols).agg(f.min("timestamp"))
        return visit_unique


    ################################## make_stats ##################################

    @class_method_logger
    def make_stats(self, visit_unique):
        num_of_users = visit_unique.select("inn").distinct().count()

        w_event_stats = Window().partitionBy("event_id")
        visit_event_stats = visit_unique.withColumn("event_user_count", f.count("inn").over(w_event_stats)) \
                                .withColumn("event_user_percent", 
                                            f.format_number(f.col("event_user_count") / num_of_users * 100, 5) \
                                             .cast(stypes.DoubleType()))

        w_group_stats = Window().partitionBy("prod_id")
        visit_group_stats = visit_event_stats.withColumn("prod_user_count", 
                                                 f.when(f.col("prod_id").isNotNull(), 
                                                        f.count("inn").over(w_group_stats))
                                                  .otherwise(None))\
                                      .withColumn("prod_user_percent", 
                                                  f.when(f.col("prod_id").isNotNull(), 
                                                         f.format_number(f.col("prod_user_count") / num_of_users * 100, 5) \
                                                          .cast(stypes.DoubleType())) \
                                                   .otherwise(None))

        w_item_stats = Window().partitionBy("item_id")
        visit_item_stats = visit_group_stats.withColumn("item_user_count", f.count("inn").over(w_item_stats)) \
                                    .withColumn("item_user_percent", 
                                                f.format_number(f.col("item_user_count") / num_of_users * 100, 5) \
                                                 .cast(stypes.DoubleType()))

        return visit_item_stats


    ################################## crete_item_dict ##################################

    @class_method_logger
    def create_item_dict(self, visit_item_stats):
        item_dict = visit_item_stats.select("hitPagePath", "eventCategory", "eventAction", "product",
                                            "event_id", "event_user_count", "event_user_percent",
                                            "prod_id", "prod_user_count", "prod_user_percent",
                                            "item_id", "item_user_count", "item_user_percent") \
                                    .distinct().cache()
        return item_dict


    ################################## save_item_dict ##################################


    @class_method_logger
    def save_table(self, item_dict):
        try:
            drop_table(self.output_scheme, self.output_table, self.hive)
            create_table_from_df(self.output_scheme, self.output_table, item_dict, self.hive)
        except:
            create_table_from_df(self.output_scheme, self.output_table, item_dict, self.hive)

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
    with CreateItemDictSbbol() as create_item_dict_sbbol:
        create_item_dict_sbbol.run()
