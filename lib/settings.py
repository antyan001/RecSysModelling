###########################################################################################
################################## RECSYS_SRORY SETTIGNS ##################################
###########################################################################################

######################### Resourses ##########################
##___________________ SCHEMES ______________________##
SBX_TEAM_DIGITCAMP = 'sbx_team_digitcamp'
DEFAULT = 'default'
PRX_EXTERNAL_GOOGLE_ANALYTICS = "prx_google_analytics_part_external_google_analytics"
EXTERNAL_GOOGLE_ANALYTICS = 'google_analytics_visit'
SKLOD_EXTERNAL_GOOGLE_ANALYTICS = "sklod_external_google_analytics"
##___________________ TABLES ______________________##
VISIT = 'visit'
# VISIT = 'tmp_visit_part'
GA_VISIT_CTL_DATE_MAP = "GA_VISIT_CTL_DATE_MAP"
GA_MA_CUSTOMER_SBBOL = "ga_ma_customer_sbbol"
##-----------------------------------------------------------------##

######################### System params ##########################

##___________________ STEP 1: TransfromClickstream ______________________##
INTERVAL_DAYPART = 64
RECSYS_STORY_CLICKSTREAM = "RECSYS_STORY_CLICKSTREAM"
##-----------------------------------------------------------------##

##___________________ STEP 2: createUserDict ______________________##
RECSYS_STORY_USER_DICT = "RECSYS_STORY_USER_DICT"
##-----------------------------------------------------------------##

##___________________ STEP 3: createItemDict ______________________##
RECSYS_STORY_ITEM_DICT = "RECSYS_STORY_ITEM_DICT"
##-----------------------------------------------------------------##

##___________________ STEP 4: createDataset ______________________##
RECSYS_STORY_DATASET_POSITIVE = "RECSYS_STORY_DATASET_POSITIVE"
RECSYS_STORY_DATASET = "RECSYS_STORY_DATASET"
EVENT_FILTER_PERCENT_L = 0.01
EVENT_FILTER_PERCENT_R = 100
EVENT_FILTER_SQL = "eventAction like '%click%'"
##-----------------------------------------------------------------##

##___________________ STEP 4.1: createTrainTesValtSet ______________________##
RECSYS_STORY_TEST = "RECSYS_STORY_TEST"
RECSYS_STORY_TRAIN = "RECSYS_STORY_DATASET_TRAIN"
RECSYS_STORY_DATASET_TRAIN_POS = "RECSYS_STORY_DATASET_TRAIN_POSITIVE"
RECSYS_STORY_DATASET_VAL = "RECSYS_STORY_DATASET_VAL"





##___________________ STEP 5: createTestSet ______________________##
RECSYS_STORY_DATASET_TARGET = "RECSYS_STORY_DATASET_TARGET"
MODEL_PARAMS = {"maxIter": 6, "alpha": 1, "nonnegative": False, "rank": 10, "regParam": 1.0}
SBBOL_PRODUCT_DICT = "sbbol_product_dict"
RECSYS_STORY_TARGET_ITEM_DICT = "RECSYS_STORY_TARGET_ITEM_DICT"
##-----------------------------------------------------------------##

##___________________ STEP 6: createALSpredictions ______________________##

ALS_ASUP_RATINGS = "ALS_ASUP_RATINGS"

##___________________ STEP 7: createPopularModel ______________________##

RECSYS_STORY_POPULAR_MODEL = "RECSYS_STORY_POPULAR_MODEL"

##___________________ STEP 8: createScalledPivotTable ______________________##
SBBOL_PRODUCT_DICT = 'sbbol_product_dict'

COL_LIST_PIVOT = ['inn', 'korpkarta', 'e_inv', 'rabota_ru', 'merchant_acquiring', 'e_acquiring', 'kredit', 'credit_card', 'bip', 'zarplata', 'LYurist', 'sberrating', 'sppu', 'customWarranty', 'mybuch_online_sso', 'sms', 'time2pay', 'samoinkass', 'deposit', 'cashOrder', 'leasing', 'loyalty_business', 'evotor', 'invoice', 'bki', 'el_sign', 'mybuch', 'compliance_helper', 'encashmentRequest', 'spasibo', 'tradeFinance_lettersOfCredit_ufs', 'yakassa', 'goz', 'SberLogist', 'guarantee', 'DocDoc', 'business_card_website', 'individual_deposit', 'currency_control', 'e_inv_podpiska_spravki', 'corporate_insurance', 'SberMarket', 'overdraft', 'pos_partner_cabinet', 'e_acquiring_fz', 'gisGmp', 'bfm2', 'e_inv_const_doc', 'crm', 'employee_verification', 'partner_bonus', 'solomoto', 'cash_management', 'school', 'mytorg', 'Ukit', 'yakassa_b2b_advertize', 'taxi', 'statement_schedule', 'clickstream', 'pledge_insurance', 'garant', 'business_travel', 'factoring', 'na_polke', 'refundVat', 'mybuch_online_ip', 'NDS_vozm', 'markets', 'property_management', 'distressed_assets', 'structuralDepositRangeAccrual', 'ofd_platforma', 'sber_business_bot', 'CooperateOnline', 'e_inv_archive', 'bestbusinessp', 'packages', 'taxFree', 'currency_control_notifications', 'InSales', 'dcdGm', 'customs_duties', 'gos_oboron_zakaz', 'derivatives', 'credit_insurance', 'brokerage', 'sber_target', 'metrics', 'depositdualcurrency', 'contcheck', 'cash_insurance', 'load_dt']


MUST_HAVE_PRODUCT_LIST = ["leasing",
                          "guarantee",
                          "rabota_ru",
                          "merchant-acquiring",
                          "zarplata",
                          "evotor",
                          "e-acquiring",
                          "mybuch",
                          "credit_card",
                          "bip",
                          "LYurist",
                          "kredit",
                          "sberrating"]

ALS_ASUP_PIVOT = "ALS_ASUP_PIVOT"

ALS_ASUP_RATINGS_SCALLED = "ALS_ASUP_RATINGS_SCALLED"

##-----------------------------------------------------------------##

##___________________ STEP 9: Partition Insert ______________________##
ALS_HISTORY_ASUP_RATINGS = "recsys_story_asup_ratings"



##___________________ STEP 10: Model Stats ______________________##

ISKRA_TABLE_NAME = "ISKRA_CVM.recsys_story_asup_ratings"
EMAIL_LIST_FILE = "mail_settings/mail_list.txt"
EMAIL_SETTINGS_FILE = "mail_settings/mail_settings.txt"

##___________________ PIPELINE: startPipelineDataset ______________________##

##-----------------------------------------------------------------##

##___________________ MODEL: ALS_model ______________________##
RECSYS_STORY_RATINGS = "RECSYS_STORY_RATINGS"

COL_USER = "user_id"
COL_ITEM = "item_id"
COL_TIMESTAMP = "timestamp"
COL_RATING = "rating"
COL_PREDICTION = "prediction"



MODEL_HEADER = {
    "userCol": COL_USER,
    "itemCol": COL_ITEM,
    "ratingCol": COL_RATING,
    "implicitPrefs": True
}



##-----------------------------------------------------------------##




##___________________ PIPELINE: startRecSysStory ______________________##
SPARK_CONFIG = {
    "process_label": "RECSYS_STORY",
    "kerberos_auth": False,
    "replication_num" : 1,
    "numofcores": 9,
    "numofinstances": 9
}
##-----------------------------------------------------------------##



##___________________ Target: product_list ______________________##


