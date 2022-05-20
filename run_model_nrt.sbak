#!/bin/bash

cd $RECSYS_STORY

printerr(){
	echo $1 >> /dev/stderr
}

#########################################
########### PREPARE DATASETS ############
#########################################

printerr $(pwd)
printerr "%%%%% START model_story_ratings_nrt/Create_NRT_sbbol_visit_part.py %%%%%"
model_story_ratings_nrt/Create_NRT_sbbol_visit_part.py


printerr "%%%%% START model_story_ratings_nrt/2_CREATE_USER_DICT.py %%%%%"
model_story_ratings_nrt/2_CREATE_USER_DICT.py


printerr "%%%%% START model_story_ratings_nrt/3_CREATE_ITEM_DICT.py %%%%%"
model_story_ratings_nrt/3_CREATE_ITEM_DICT.py


printerr "%%%%% START model_story_ratings_nrt/4_CREATE_DATASET.py %%%%%"
model_story_ratings_nrt/4_CREATE_DATASET.py


printerr "%%%%% START model_story_ratings_nrt/5_CREATE_TARGET_DATASET.py %%%%%"
model_story_ratings_nrt/5_CREATE_TARGET_DATASET.py


# printerr "%%%%% START model_story_ratings_nrt/6_ALS_PREDICTION.py %%%%%"
# model_story_ratings_nrt/6_ALS_PREDICTION.py


# printerr "%%%%% START model_story_ratings/Create_popular_model.py %%%%%"
# model_story_ratings_nrt/Create_popular_model.py


# printerr "%%%%% START model_story_ratings_nrt/7_RESCYS_ASUP_RATING_TRAMSFORM.py %%%%%"
# model_story_ratings_nrt/7_RESCYS_ASUP_RATING_TRAMSFORM.py


#########################################
############# INSERT TABLE ##############
#########################################


# printerr "%%%%% START model_story_ratings/8_TABLE_PARTITION_INSERT.py %%%%%"
# model_story_ratings/8_TABLE_PARTITION_INSERT.py


# printerr "%%%%% START model_story_ratings/9_EXPORT_TO_ORACLE.py %%%%%"
# model_story_ratings_nrt/9_EXPORT_TO_ORACLE.py


# printerr "%%%%% START model_story_ratings/10_MODEL_SBBOL_STATS.py %%%%%"
# model_story_ratings_nrt/10_MODEL_SBBOL_STATS.py