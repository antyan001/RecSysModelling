#!/bin/env python3


import os, sys

recsys_cf = os.environ.get("RECSYS_STORY")
sys.path.append(recsys_cf)
os.chdir(recsys_cf)

from lib.tools import *
from datetime import datetime, timedelta
import loader as load




class ExportToOracle():


    ################################## init ##################################


    def __init__(self):
        # self.REFDATE = "2019-09-01"
        self.script_name = "SBBOLEXPORTTOORACLE"
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')

        self.input_scheme = SBX_TEAM_DIGITCAMP
        self.als_asup_ratings_scalled = ALS_ASUP_RATINGS_SCALLED

        self.output_scheme = SBX_TEAM_DIGITCAMP
        self.history_asup_ratings = ALS_HISTORY_ASUP_RATINGS



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
                        numofcores=15,
                        numofinstances=15)

        self.hive = self.sp.sql


    ################################## run ##################################
    @class_method_logger
    def run(self):
        self.export_to_oracle()


    ################################## export_to_oracle ##################################


    def export_to_oracle(self):
        conn_schema = self.output_scheme
        old_table_name = self.als_asup_ratings_scalled
        sdf = self.hive.table("{}.{}".format(conn_schema, old_table_name))
        sdf = sdf.select(*[f.col(col).alias(col.upper()) for col in sdf.columns])

        #dropping olt table
        db = OracleDB('iskra4')
        db.connect()

        table_name = ISKRA_TABLE_NAME
        sql = "DROP TABLE {}".format(table_name)
        try:
            db.cursor.execute(sql)
            db.connection.commit()
            log("# __oracle_dropping__ : done", self.logger)
        except:
            log("# __oracle_dropping__ : nothing to drop", self.logger)
            
        

        typesmap={}
        for column_name, column in sdf.dtypes:
            if column == 'string':
                if 'INN' in column_name.upper() or 'KPP' in column_name:
                    typesmap[column_name] = 'VARCHAR(20)'
                elif 'PRODUCTS'.upper() in column_name:
                    typesmap[column_name] = 'VARCHAR(4000)'
                else:
                    typesmap[column_name] = 'VARCHAR(70)'
            elif column == 'int':
                typesmap[column_name] = 'INTEGER'
            elif column == 'bigint':
                typesmap[column_name] = 'INTEGER'
            elif column == 'timestamp':
                typesmap[column_name] = 'DATE'
            elif column == 'float' or column == 'double':
                typesmap[column_name] = 'FLOAT'
            else:
                None

        
        ##create new_table
        cols = ', '.join([col + ' ' + typesmap[col] for col in sdf.columns])
        db = OracleDB('iskra4')
        mode = 'append'
        sdf \
            .write \
            .format('jdbc') \
            .mode(mode) \
            .option('url', 'jdbc:oracle:thin:@//'+db.dsn) \
            .option('user', db.user) \
            .option('password', db.password) \
            .option('dbtable', table_name) \
            .option('createTableColumnTypes', cols)\
            .option('driver', 'oracle.jdbc.driver.OracleDriver') \
            .save()

        log("# __oracle_export__ : done", self.logger)



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
    with ExportToOracle() as export_to_oracle:
        export_to_oracle.run()
