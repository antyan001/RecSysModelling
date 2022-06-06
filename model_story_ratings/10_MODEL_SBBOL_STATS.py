#!/bin/env python3


import os, sys

recsys_cf = os.environ.get("RECSYS_STORY")
sys.path.append(recsys_cf)
os.chdir(recsys_cf)

from lib.tools import *
from datetime import datetime, timedelta
import loader as load
from lib.mail_sender import *
import json




class CreateModelStoryStats():


    ################################## init ##################################


    def __init__(self):
        self.script_name = "MODEL_SBBOL_STORY_STATS"
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')

        self.iskra_table_name = ISKRA_TABLE_NAME

        self.email_settings_filepath = EMAIL_SETTINGS_FILE
        self.receivers_filepath = EMAIL_LIST_FILE



        self.init_logger()
        log("# __init__ : begin", self.logger)
        # self.start_spark()
        self.connect_iskra()


    ############################## Spark ##############################


    @class_method_logger
    def start_spark(self):
        self.sp = spark(schema=SBX_TEAM_DIGITCAMP,
                        process_label=self.script_name,
                        replication_num=1,
                        kerberos_auth=False,
                        numofcores=10,
                        numofinstances=10)

        self.hive = self.sp.sql

    
    ############################## Iskra ##############################

    @class_method_logger
    def connect_iskra(self):
#         self.loader = load.Loader(init_dsn=True, encoding='cp1251',  sep=',')
#         self.data_base = load.OracleDB('', '', self.loader._get_dsn('')) 
        self.data_base = OracleDB('iskra4')
        self.data_base.connect()


    ################################## run ##################################


    @class_method_logger
    def run(self):
        msg = self.get_message()
        self.send_message(msg)


    ################################## get stats ##################################

    @class_method_logger
    def get_max_load_date(self):
        query = '''
                SELECT max(LOAD_DT) AS MAX_DATE
                FROM {}
                WHERE inn is not Null
                '''.format(self.iskra_table_name)
        max_date_pd = pd.read_sql(query, con=self.data_base.connection)
        max_date_load = max_date_pd["MAX_DATE"][0]
        return max_date_load


    def get_inn_cnt_for_date(self, date):
        query_for_inn_cnt = """
                            SELECT count(distinct INN) AS INN_CNT_DIST
                            FROM {}
                            WHERE inn is not Null
                            and LOAD_DT = '{}'
                            """.format(self.iskra_table_name, date)
        inn_cnt_dist_pd = pd.read_sql(query_for_inn_cnt, con=self.data_base.connection)
        inn_cnt_dist = inn_cnt_dist_pd["INN_CNT_DIST"][0]
        return inn_cnt_dist


    ################################## get message ##################################


    def get_message(self):
        msg = ""

        max_date_load = self.get_max_load_date()
        inn_cnt_dist = self.get_inn_cnt_for_date(max_date_load)

        msg = """
ISKRA:
Последняя дата загрузки: {}
Число уникальных ИНН на последнюю дату: {}
""".format(max_date_load, inn_cnt_dist)
        return msg


    ################################## send message ##################################


    def send_message(self, msg):
        with open(self.email_settings_filepath, "r") as f:
            settings = json.load(f)

        with open(self.receivers_filepath, "r") as f:
            recivers_str = f.read()
            receivers = recivers_str.split('\n')
        
        authorization = Authorization(user=settings["user"],
                                      domain='ALPHA',
                                      mailbox=settings["mailbox"],
                                      server='cab-vop-mbx2053.omega.sbrf.ru')

        mail_receiver = MailReceiver(settings["password"], auth_class=authorization)

        mail_receiver.send_message(receivers, 'SBBOL_STORY_RATINGS_STATS {}'.format(self.currdate), msg)


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
    with CreateModelStoryStats() as model_story_stats:
        model_story_stats.run()
