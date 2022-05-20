#!/bin/env python3


import os, sys

recsys_cf = os.environ.get("RECSYS_STORY")
sys.path.append(recsys_cf)
os.chdir(recsys_cf)

from lib.tools import *
from datetime import datetime, timedelta




class CreateSbbolUserDictNrt():


    ################################## init ##################################


    def __init__(self):
        # self.REFDATE = "2019-09-01"
        self.script_name = "CREATE_USER_DICT_SBBOL_NRT"
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')

        self.input_scheme = SBX_TEAM_DIGITCAMP
        self.input_table = "RECSYS_STORY_CLICKSTREAM_NRT"

        self.output_scheme = SBX_TEAM_DIGITCAMP
        self.output_table = "RECSYS_STORY_USER_DICT_NRT"

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
        user_dict = self.create_user_id(clickstream)
        self.save_table(user_dict)


    ################################## load_table ##################################

    @class_method_logger
    def load_clickstream(self):
        clickstream = self.hive.sql("""SELECT *
                                       FROM {schema}.{table}
                                    """.format(schema=self.input_scheme,
                                               table=self.input_table))
        return clickstream

    
    ################################## create_user_id ##################################

    @class_method_logger
    def create_user_id(self, clickstream):
        sbbol_user_id = clickstream.select("inn",
                                           "sbbolUserId").distinct()\
                                           .filter("inn not like '%000000%'").cache()
        w = Window().orderBy("inn")
        user_dict = sbbol_user_id.withColumn("user_id", f.dense_rank().over(w))
        return user_dict


    ################################## save_table ##################################


    @class_method_logger
    def save_table(self, user_dict):
        try:
            drop_table(self.output_scheme , self.output_table, self.hive)
            create_table_from_df(self.output_scheme , self.output_table, user_dict, self.hive)
        except:
            create_table_from_df(self.output_scheme , self.output_table, user_dict, self.hive)

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
    with CreateSbbolUserDictNrt() as create_user_dict_nrt:
        create_user_dict_nrt.run()
