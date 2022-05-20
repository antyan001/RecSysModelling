#!/bin/env python3


import os, sys

recsys_cf = os.environ.get("RECSYS_STORY")
sys.path.append(recsys_cf)
os.chdir(recsys_cf)

from lib.tools import *
from datetime import datetime, timedelta




class CreateTargetDataset():


    ################################## init ##################################


    def __init__(self):
        # self.REFDATE = "2019-09-01"
        self.script_name = "SBBOL_CREATE_TARGET_DATASET"
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')

        self.input_cheme = SBX_TEAM_DIGITCAMP
        self.dataset_table = RECSYS_STORY_DATASET_POSITIVE
        self.product_dict_table = SBBOL_PRODUCT_DICT
        self.item_dict_table = RECSYS_STORY_ITEM_DICT
        self.user_dict_table = RECSYS_STORY_USER_DICT

        self.output_cheme = SBX_TEAM_DIGITCAMP
        self.dataset_target_table = RECSYS_STORY_DATASET_TARGET
        self.item_dict_target_table = RECSYS_STORY_TARGET_ITEM_DICT

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

        dataset = self.load_dataset()
        item_dict = self.load_item_dict()
        sbbol_product_dict = self.load_product_dict()
        user_dict = self.load_user_dict()

        target_item_prod_dict, ITEM_LIST = self.create_target_dict(item_dict, sbbol_product_dict)
        target_dataset = self.create_target_dataset(dataset, ITEM_LIST)
        target_full_dataset = self.create_full_targer_dataset(target_dataset, user_dict)

        self.save_target_dataset(target_full_dataset)
        self.save_target_item_dict(target_item_prod_dict)


    ################################## load_table ##################################


    @class_method_logger
    def load_dataset(self):
        dataset = load_table(self.input_cheme, self.dataset_table, self.hive).cache()
        return dataset


    @class_method_logger
    def load_item_dict(self):
        item_dict = load_table(self.input_cheme, self.item_dict_table, self.hive).cache()
        return item_dict


    @class_method_logger
    def load_user_dict(self):
        user_dict = load_table(self.input_cheme, self.user_dict_table, self.hive).cache()
        return user_dict


    @class_method_logger
    def load_product_dict(self):
        sbbol_product_dict = load_table(self.input_cheme, self.product_dict_table, self.hive).cache()
        return sbbol_product_dict


    ################################## create_target_dict ##################################


    @class_method_logger
    def create_target_dict(self, item_dict, sbbol_product_dict):
        product_item_dict = item_dict.filter(f.col("product") != '').select("product", "item_id").distinct()
        target_item_prod_dict = product_item_dict.join(sbbol_product_dict,
                                               f.upper(sbbol_product_dict.prod_cd_asup) == f.upper(product_item_dict.product),
                                               "inner")\
                                         .select("item_id", "prod_cd_asup").distinct().cache()

        product_cnt = target_item_prod_dict.count()
        log("# products_cnt = {}".format(product_cnt), self.logger)

        ITEM_LIST = target_item_prod_dict.select("item_id").toPandas()["item_id"].tolist()

        return target_item_prod_dict, ITEM_LIST


    ################################## create_target_dataset ##################################


    @class_method_logger
    def create_target_dataset(self, dataset, ITEM_LIST):
        target_dataset = dataset.filter(f.col("item_id").isin(ITEM_LIST))
        return target_dataset

    ################################## create_target_dataset_full ##################################


    @class_method_logger
    def create_full_targer_dataset(self, pos_target_dataset, user_dict):
        items = pos_target_dataset.select("item_id").distinct()
        users = user_dict.select("user_id").distinct()

        user_item_cross = users.crossJoin(items)
        target_dataset_full = user_item_cross.join(pos_target_dataset, 
                                              on=['user_id', 'item_id'], 
                                              how="left_outer") \
                                        .na.fill({"rating" : 0.0})
        return target_dataset_full

    ################################## save_table ##################################


    @class_method_logger
    def save_target_dataset(self, target_dataset):
        try:
            drop_table(self.output_cheme, self.dataset_target_table, self.hive)
            create_table_from_df(self.output_cheme, self.dataset_target_table, target_dataset, self.hive)
        except:
            create_table_from_df(self.output_cheme, self.dataset_target_table, target_dataset, self.hive)

    
    @class_method_logger
    def save_target_item_dict(self, target_item_prod_dict):
        try:
            drop_table(self.output_cheme, self.item_dict_target_table, self.hive)
            create_table_from_df(self.output_cheme, self.item_dict_target_table, target_item_prod_dict, self.hive)
        except:
            create_table_from_df(self.output_cheme, self.item_dict_target_table, target_item_prod_dict, self.hive)

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
    with CreateTargetDataset() as create_target_dataset:
        create_target_dataset.run()
