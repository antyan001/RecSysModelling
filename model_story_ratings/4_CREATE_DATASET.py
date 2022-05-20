#!/bin/env python3


import os, sys

recsys_cf = os.environ.get("RECSYS_STORY")
sys.path.append(recsys_cf)
os.chdir(recsys_cf)

from lib.tools import *
from datetime import datetime, timedelta




class CreateSbbolDataset():


    ################################## init ##################################


    def __init__(self):
        # self.REFDATE = "2019-09-01"
        self.script_name = "CREATESBBOLDATASET"
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')

        self.input_scheme = SBX_TEAM_DIGITCAMP
        self.clickstreame_table = RECSYS_STORY_CLICKSTREAM
        self.item_dict_table = RECSYS_STORY_ITEM_DICT
        self.user_dict_table = RECSYS_STORY_USER_DICT

        self.output_scheme = SBX_TEAM_DIGITCAMP
        self.full_dataset_table = RECSYS_STORY_DATASET
        self.pos_dataset_table = RECSYS_STORY_DATASET_POSITIVE

        self.percent_l = 0
        self.percent_r = 100

        self.full_dataset_flg = False


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
                        numofcores=11,
                        numofinstances=11)

        self.hive = self.sp.sql


    ################################## run ##################################
    @class_method_logger
    def run(self):

        clickstream = self.load_clickstream()
        user_dict = self.load_user_dict()
        item_dict = self.load_item_dict()

        visit_user_item = self.join_user_item_id(clickstream, user_dict, item_dict)
        visit_user_item_filt = self.filter_action(visit_user_item)
        visit_user_item_filt_unique = self.take_first_action(visit_user_item_filt)
        pos_dataset = self.create_pos_dataset(visit_user_item_filt_unique)
        self.save_pos_dataset(pos_dataset)

        if self.full_dataset_flg:
            full_dataset = self.create_full_dataset(pos_dataset, item_dict)
            self.save_full_dataset(full_dataset)







    ################################## load_tables ##################################


    @class_method_logger
    def load_clickstream(self):
        clickstream = load_table(self.input_scheme, self.clickstreame_table, self.hive).cache()
        return clickstream


    @class_method_logger
    def load_user_dict(self):
        user_dict = load_table(self.input_scheme, self.user_dict_table, self.hive).cache()
        return user_dict.select("inn", "user_id").distinct()


    @class_method_logger
    def load_item_dict(self):
        item_dict = load_table(self.input_scheme, self.item_dict_table, self.hive).cache()
        return item_dict


    ################################## join_user_item_id ##################################


    @class_method_logger
    def join_user_item_id(self, clickstream, user_dict, item_dict):
        clickstream_user = clickstream.join(user_dict, on="inn", how="inner")
        na_dict = {"product": "<None>",  "eventCategory": "<None>"}
        visit_user_item = clickstream_user.na.fill(na_dict) \
                                          .join(item_dict.na.fill(na_dict), 
                                                on=["eventAction", "eventCategory", "hitPagePath", "product"], 
                                                how="inner") \
                                          .withColumn("product", f.regexp_replace("product", "<None>", None)) \
                                          .withColumn("hitPagePath", f.regexp_replace("hitPagePath", "<None>", None))\
                                          .withColumn("eventCategory", f.regexp_replace("eventCategory", "<None>", None)).cache()
        return visit_user_item


    ################################## filter_action ##################################


    @class_method_logger
    def filter_action(self, visit_user_item):
        us_num = visit_user_item.select("inn").distinct().count()
        cond = (
                (f.col("item_user_percent") <= self.percent_r) &
                (f.col("item_user_percent") >= self.percent_l) 
                )    
        visit_user_item_filt = visit_user_item.filter(cond).cache()
        return visit_user_item_filt


    ################################## take_first_action ##################################


    @class_method_logger
    def take_first_action(self, visit_user_item_filt):
        visit_user_item_filt_unique = visit_user_item_filt.groupBy("user_id", "item_id")\
                                                    .agg(f.min("timestamp").alias("timestamp")).cache()
        return visit_user_item_filt_unique


    ################################## create_pos_dataset ##################################

    
    @class_method_logger
    def create_pos_dataset(self, visit_user_item_filt_unique):
        pos_dataset = visit_user_item_filt_unique.select("user_id",
                                               "item_id", 
                                               f.col("timestamp").cast(stypes.LongType())) \
                                        .distinct() \
                                        .withColumn("rating", f.lit(1.0))
        return pos_dataset


    ################################## create_full_dataset ##################################

    
    @class_method_logger
    def create_full_dataset(self, pos_dataset, item_dict):
        cond = (
                (f.col("item_user_percent") <= self.percent_r) &
                (f.col("item_user_percent") >= self.percent_l) 
                )
        item_dict_filt = item_dict.filter(cond)
        items_filt = item_dict_filt.select("item_id").distinct()
        users_filt = pos_dataset.select("user_id").distinct()

        user_item_cross = users_filt.crossJoin(items_filt)
        user_item_full = user_item_cross.join(pos_dataset, 
                                              on=['user_id', 'item_id'], 
                                              how="left_outer") \
                                        .na.fill({"rating" : 0.0})
        return user_item_full

    ################################## save_tables ##################################


    @class_method_logger
    def save_pos_dataset(self, pos_dataset):
        try:
            drop_table(self.output_scheme, self.pos_dataset_table , self.hive)
            create_table_from_df(self.output_scheme, self.pos_dataset_table, pos_dataset, self.hive)
        except:
            create_table_from_df(self.output_scheme, self.pos_dataset_table, pos_dataset, self.hive)

    @class_method_logger
    def save_full_dataset(self, full_dataset):
        try:
            drop_table(self.output_scheme, self.full_dataset_table , self.hive)
            create_table_from_df(self.output_scheme, self.full_dataset_table, full_dataset, self.hive)
        except:
            create_table_from_df(self.output_scheme, self.full_dataset_table, full_dataset, self.hive)

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
    with CreateSbbolDataset() as create_sbbol_dataset:
        create_sbbol_dataset.run()
