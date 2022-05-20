#!/bin/env python3


import os, sys

recsys_cf = os.environ.get("RECSYS_STORY")
sys.path.append(recsys_cf)
os.chdir(recsys_cf)

from lib.tools import *
from datetime import datetime, timedelta
import loader as load




class PartitionInsert():


    ################################## init ##################################


    def __init__(self):
        # self.REFDATE = "2019-09-01"
        self.script_name = "SBBOL_PARTITION_INSERT"
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')

        self.input_scheme = SBX_TEAM_DIGITCAMP
        self.als_asup_ratings_table = ALS_ASUP_RATINGS_SCALLED

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
                        kerberos_auth=True,
                        numofcores=15,
                        numofinstances=15)

        self.hive = self.sp.sql
        self.hive.setConf("hive.exec.dynamic.partition","true")
        self.hive.setConf("hive.exec.dynamic.partition.mode","nonstrict")
        self.hive.setConf("hive.enforce.bucketing","false")
        self.hive.setConf("hive.enforce.sorting","false")
        self.hive.setConf("spark.sql.sources.partitionOverwiteMode","dynamic")
        self.hive.setConf("hive.load.dynamic.partitions.thread", 1)


    ################################## run ##################################
    @class_method_logger
    def run(self):
        ratings_sdf = self.load_ratings_table()
        self.insert_partition_table(ratings_sdf)


    ################################## load_table ##################################


    @class_method_logger
    def load_ratings_table(self):
        ratings_sdf = load_table(self.input_scheme, self.als_asup_ratings_table, self.hive)
        return ratings_sdf


    ################################## partition_insert ##################################


    @class_method_logger
    def createSDF(self, conn_schema, target_tbl, insert, part_cols_str, bucket_num, bucket_cols):

        self.hive.sql('''create table {schema}.{tbl} (
                                                {fields}
                                                    )
                    PARTITIONED BY ({part_col_lst})
                    CLUSTERED BY ({bucket_cols}) INTO {bucket_num} BUCKETS STORED AS PARQUET
                '''.format(schema=conn_schema,
                            tbl=target_tbl,
                            fields=insert,
                            part_col_lst=part_cols_str,
                            bucket_num=bucket_num,
                            bucket_cols=bucket_cols)
                )


    @class_method_logger
    def insertToSDF(self, sdf, conn_schema, tmp_tbl, target_tbl, part_cols_str, bucket_cols):
        sdf.registerTempTable(tmp_tbl)
        
        self.hive.sql("""
        insert into table {schema}.{tbl}
        partition({part_col})
        select * from {tmp_tbl}
        cluster by ({bucket_cols})
        """.format(schema=conn_schema,
                tbl=target_tbl,
                tmp_tbl=tmp_tbl,
                part_col=part_cols_str,
                bucket_cols=bucket_cols)
                ) 


    @class_method_logger
    def insert_partition_table(self, sort_sdf):
        conn_schema = self.output_scheme
        table_name = 'recsys_story_asup_ratings_new'
        old_table_name = self.history_asup_ratings

        bucket_col = ["inn"]
        bucket_num = 125

        if bucket_col is not None:
            bucket_cols = ', '.join([col for col in bucket_col])

        part_tupl_lst = [('load_dt', 'string')]
        part_tupl_str = ', '.join(["{} {}".format(col, _type) for col, _type in part_tupl_lst])
                                
        self.hive.sql("drop table if exists {schema}.{tbl} purge".format(schema=conn_schema, tbl=table_name))
        insert = ', '.join(["{} {}".format(col, _type) for col, _type in sort_sdf.dtypes if col.lower() not in part_tupl_lst[0][0]])

        self.createSDF(conn_schema, 
                       target_tbl=table_name, 
                       insert=insert, 
                       part_cols_str=part_tupl_str,
                       bucket_num=bucket_num,
                       bucket_cols=bucket_cols
                        )
        self.insertToSDF(sort_sdf,
                         conn_schema='sbx_team_digitcamp',
                         tmp_tbl='tmp_sort_sdf', 
                         target_tbl=table_name, 
                         part_cols_str=part_tupl_lst[0][0],
                         bucket_cols=bucket_cols)
        self.hive.sql("drop table if exists {0}.{1} purge".format(conn_schema, old_table_name))
        self.hive.sql("ALTER TABLE {0}.{1} rename to {0}.{2}".format(conn_schema, table_name, old_table_name))
        log("# __partition_insert__ : done", self.logger)


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
    with PartitionInsert() as partition_insert:
        partition_insert.run()
