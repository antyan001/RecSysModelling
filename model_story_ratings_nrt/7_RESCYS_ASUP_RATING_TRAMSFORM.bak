#!/bin/env python3

import os, sys

recsys_cf = os.environ.get("RECSYS_STORY")
sys.path.append(recsys_cf)
os.chdir(recsys_cf)

from lib.tools import *
from pyspark.sql.types import *
import time
# import datetime




class AsupRatingsTransformNrt():


    ################################## init ##################################


    def __init__(self):
        self.script_name = "ALS_ASUP_RATINGS_TRANSFORM_NRT"
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')

        self.input_scheme = SBX_TEAM_DIGITCAMP
        self.rating_table = "RECSYS_STORY_RATINGS_NRT"
        self.user_dict_table = "RECSYS_STORY_USER_DICT_NRT"
        self.target_item_dict = "RECSYS_STORY_TARGET_ITEM_DICT_NRT"
        self.sbbol_prod_table = SBBOL_PRODUCT_DICT

        self.out_col_list = COL_LIST_PIVOT

        self.output_scheme = SBX_TEAM_DIGITCAMP
        self.als_asup_ratings_transform = "ALS_ASUP_RATINGS_SCALLED_NRT"
        # self.als_asup_pivot = ALS_ASUP_PIVOT
        
        self.popular_flg = False
        
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
                        numofcores=10,
                        numofinstances=10)

        self.hive = self.sp.sql


    ################################## run ##################################


    @class_method_logger
    def run(self):
        als_predictions = self.load_als_predictions()
        user_dict = self.load_user_dict()
        target_item_prod_dict = self.load_target_prod_dict()

        predictions_product = self.join_products(als_predictions, target_item_prod_dict)

        prediction_products_inn = self.join_inn(predictions_product, user_dict)

        als_asup_prediction = self.create_load_dt(prediction_products_inn)

        min_max_predictions = self.min_max_scalling(als_asup_prediction)

        final_table = self.sigmoid_ratings(min_max_predictions)
        
        if self.popular_flg == False:
            self.save_table(final_table)
        else:
            all_inn_sdf = final_table.select("inn").distinct()
            popular_ratings = self.create_popular_model_inn()
            popular_ratings_not_active = popular_ratings.join(all_inn_sdf.alias("finn"),
                                                              "inn",
                                                              how="left")\
                                                        .filter(f.col("finn.inn").isNull())
            final_table_with_popular = final_table.union(popular_ratings_not_active)
            self.save_table(final_table_with_popular)

        
    ################################## load_table ##################################


    @class_method_logger
    def load_als_predictions(self):
        als_predictions = load_table(self.input_scheme, self.rating_table, self.hive).cache()
        return als_predictions


    @class_method_logger
    def load_user_dict(self):
        user_dict = load_table(self.input_scheme, self.user_dict_table, self.hive).select("inn", "user_id")\
                                                                                  .distinct().cache()
        return user_dict


    @class_method_logger
    def load_target_prod_dict(self):
        target_item_prod_dict = load_table(self.input_scheme, self.target_item_dict, self.hive).cache()
        return target_item_prod_dict


    ################################## join_products ##################################


    @class_method_logger
    def join_products(self, als_predictions, target_item_prod_dict):
        predictions_product = als_predictions.join(target_item_prod_dict,
                                                   target_item_prod_dict.item_id == als_predictions.item_id,
                                                   "inner")\
                                             .select("user_id",
                                                     "prod_cd_asup",
                                                     "timestamp",
                                                     f.col("prediction").alias("ratings_als"))
        return predictions_product


    ################################## join_inn ##################################


    @class_method_logger
    def join_inn(self, predictions_product, user_dict):
        prediction_products_inn = predictions_product.join(user_dict,
                                                   user_dict.user_id == predictions_product.user_id,
                                                  "inner")\
                                             .select("inn",
                                                     "prod_cd_asup",
                                                     "timestamp",
                                                     "ratings_als").cache()
        prediction_products_inn_correct = prediction_products_inn.filter("inn not like '%0000%'")
        return prediction_products_inn_correct

    
    ################################## create_load_dt ##################################


    @class_method_logger
    def create_load_dt(self, prediction_products_inn):
        now_time = datetime.fromtimestamp(time.time())
        als_asup_prediction = prediction_products_inn.withColumn("LOAD_DT",
                                                                 f.unix_timestamp(f.lit(now_time)).cast("timestamp"))
        return als_asup_prediction


    ################################## min_max_scalling ##################################


    @class_method_logger
    def min_max_scalling(self, sdf):
        minmax_ = sdf.groupBy('inn').agg(f.min('ratings_als').alias('min_ratings_als'),
                                         f.max('ratings_als').alias('max_ratings_als'))
        sdf = minmax_.join(sdf, on='inn').select('inn', 
                                                 'prod_cd_asup',
                                                 ((f.col('ratings_als') - f.col('min_ratings_als')) \
                                                 / (f.col('max_ratings_als') - f.col('min_ratings_als'))).alias('ratings_min_max')
                                                )
        return sdf

    
    ################################## sigmoid_ratings ##################################

    @class_method_logger
    def sigmoid_ratings(self, sdf):
        sigmoid_ratings_sdf = sdf.withColumn("exp_ratings", 1 / (1 + f.exp(-1 * f.col("ratings_min_max"))))
        ratings_ord_sdf = sigmoid_ratings_sdf.orderBy(f.col("inn"), f.col("exp_ratings").desc())
        ratings_dt_sdf = ratings_ord_sdf.withColumn('load_dt', f.lit(f.current_date().cast(StringType())))
        ratings_final_sdf = ratings_dt_sdf.select("inn",
                                                  "prod_cd_asup",
                                                  f.round(f.col("exp_ratings"), 4).alias("ratings"),
                                                  "load_dt")
        return ratings_final_sdf



    @class_method_logger
    def sigmoid_ratings_pivot(self, sdf):
        res = sdf.rdd.map(lambda row: ((row.asDict()['inn']),
                               [row.asDict()['prod_cd_asup'],
                                float(1./(1. + np.exp(-row.asDict()['ratings_min_max'])))
                               ])
                 )\
                 .groupByKey().map(lambda x: (x[0], sorted(list(x[1]), key=lambda xx: xx[1], reverse=True)))

        out = res.toDF(schema=StructType([StructField('inn', StringType()),
                                  StructField('arr', ArrayType(StructType([ StructField('prod_cd_asup', StringType()),
                                                                            StructField('ratings_sigm', FloatType())
                                                                          ])))
                                 ])
              )
        return out


    ################################## Pivoting_table ##################################


    @class_method_logger
    def pivoting_table(self, out):
        sorted_prod_cd_asup = ['inn']+[item.prod_cd_asup for item in out.take(1)[0]['arr']]
        sort_sdf = out.rdd.map(lambda r: Row(inn=r.inn, **{s.prod_cd_asup: s.ratings_sigm for s in r.arr})).toDF()
        sort_sdf = sort_sdf.select(*sorted_prod_cd_asup)
        sort_sdf = sort_sdf.withColumn('load_dt', f.lit(f.current_date().cast(StringType())))
        renamed_cols = [f.col(col).alias(re.sub('\-','_',col)) for col in sort_sdf.columns]
        sort_sdf=sort_sdf.select(*renamed_cols)     
        return sort_sdf

    
    ################################## create_missed_columns ##################################


    @class_method_logger
    def create_missed_columns(self, sdf):
        asup_all_cols = list(set(self.out_col_list)-set(['inn', 'load_dt']))
        recsys_asup_cols = list(set(sdf.columns)-set(['inn', 'load_dt']))

        unpopular_asup_prods = list(set(asup_all_cols) - set(recsys_asup_cols))
        unpopular_asup_prods = list(map(lambda x: x.lower(), unpopular_asup_prods))

        log(unpopular_asup_prods, self.logger)

        for cols in unpopular_asup_prods:
            sdf = sdf.withColumn(cols, f.lit(None).cast(DoubleType()))

        sdf = sdf.select(self.out_col_list)
        return sdf


    ################################## create_popular_model ##################################


    @class_method_logger
    def create_popular_model_inn(self):
        #load popular model
        popular_model_ratings = load_table(SBX_TEAM_DIGITCAMP, RECSYS_STORY_POPULAR_MODEL, self.hive)
        #load_all_sbbol_users
        ISKRA_TABLE = "ISKRA_CVM.ma_cmdm_ma_customer_sbbol"
        db = OracleDB('iskra4')
        sbbol_customer = self.hive.read.format('jdbc')\
                                        .option('url', 'jdbc:oracle:thin:@//' + db.dsn) \
                                        .option('user', db.user) \
                                        .option('password', db.password) \
                                        .option('dbtable', ISKRA_TABLE) \
                                        .option('driver', 'oracle.jdbc.driver.OracleDriver')\
                             .load()

        all_sbbol_users_inn = sbbol_customer.select("CU_INN").distinct()
        sbbol_popular_model_ratings = all_sbbol_users_inn.withColumn("inn", f.col("CU_INN"))\
                                                         .crossJoin(popular_model_ratings)\
                                                         .drop(f.col("CU_INN"))
        return sbbol_popular_model_ratings

    ################################## save_table ##################################


    @class_method_logger
    def save_table(self, final_table):
        try:
            drop_table(self.output_scheme, self.als_asup_ratings_transform, self.hive)
            create_table_from_df(self.output_scheme, self.als_asup_ratings_transform, final_table, self.hive)
        except:
            create_table_from_df(self.output_scheme, self.als_asup_ratings_transform, final_table, self.hive)

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
    with AsupRatingsTransformNrt() as asup_ratings_transform:
        asup_ratings_transform.run()
