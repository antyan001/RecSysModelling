#!/bin/env python3


import os, sys

recsys_cf = os.environ.get("RECSYS_STORY")
sys.path.append(recsys_cf)
os.chdir(recsys_cf)

from lib.tools import *
from datetime import datetime, timedelta

# Preproc
from sklearn.preprocessing import MinMaxScaler

# Metrics
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.metrics import roc_auc_score, log_loss
from sklearn.metrics import roc_curve, precision_recall_curve
from sklearn.metrics import confusion_matrix, classification_report
from sklearn.metrics import accuracy_score, precision_score, recall_score,  f1_score

# Model
from pyspark.ml.recommendation import ALS, ALSModel




class SbbolAlsPredictionNrt():


    ################################## init ##################################


    def __init__(self):
        self.script_name = "ALS_SBBOL_PREDICTION_NRT"
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')

        self.input_scheme = SBX_TEAM_DIGITCAMP
        self.train_table = "RECSYS_STORY_DATASET_POSITIVE_NRT"
        self.test_table = "RECSYS_STORY_DATASET_TARGET_NRT"

        self.model_header = MODEL_HEADER
        self.model_params = MODEL_PARAMS

        self.output_scheme = SBX_TEAM_DIGITCAMP
        self.rating_table = "RECSYS_STORY_RATINGS_NRT"

        self.init_logger()
        log("# __init__ : begin", self.logger)
        self.start_spark()
        self.als_init()


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


    ################################## als_init ##################################


    @class_method_logger
    def als_init(self):
        self.als = ALS(
                        **self.model_header,
                        **self.model_params
                       )


    ################################## run ##################################


    @class_method_logger
    def run(self):
        train_sdf = self.load_train_dataset()
        test_sdf = self.load_test_dataset()

        self.model = self.als.fit(train_sdf)
        pred_sdf = self.model.transform(test_sdf)
        self.save_als_ratings(pred_sdf)


    ################################## load_table ##################################


    @class_method_logger
    def load_train_dataset(self):
        train_sdf = load_table(self.input_scheme, self.train_table, self.hive).cache()
        return train_sdf


    @class_method_logger
    def load_test_dataset(self):
        test_sdf = load_table(self.input_scheme, self.test_table, self.hive).cache()
        return test_sdf


    ################################## save_ratings ##################################


    @class_method_logger
    def save_als_ratings(self, pred_sdf):
        try:
            drop_table(self.output_scheme, self.rating_table, self.hive)
            create_table_from_df(self.output_scheme, self.rating_table, pred_sdf, self.hive)
        except:
            create_table_from_df(self.output_scheme, self.rating_table, pred_sdf, self.hive)

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
    with SbbolAlsPredictionNrt() as als_sbbol_prediction:
        als_sbbol_prediction.run()
