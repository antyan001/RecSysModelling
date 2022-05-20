import pyspark.sql.functions as f


class Recsys360TrainTestValRangeSplit():
    """
Класс для разбиения датасета на трейн тест в моделях рекомендаций 
    
Parameters
----------
dataset_pos : pyspark.sql.dataframe.DataFrame
    датасет со всеми позитивными событиями
    Входные поля:
        ('user_id', 'int'),
        ('item_id', 'int'),
        ('timestamp', 'bigint'),
        ('rating', 'double')
    
target_list : list
    список из id целевых событий, на которых планируем смотреть метрики

test_size : float or None, default 0.3
    доля юзеров с целевыми событиями, которые попадут в test

val_flg : bool or None, default False
    задает наличие валидационного датасета
    если True, то делит тест пополам по количеству юзеров
    """
    
    def __init__(
        self,
        dataset_pos,
        dataset_target,
        target_list,
        test_size=0.3,
        val_flg=False
    ):
        self.dataset_pos = dataset_pos
        self.dataset_target = dataset_target
        self.target_list = target_list
        self.val_flg = val_flg
        self.test_size = test_size
        
    def split(self):
        """
        Возвращает зануленный full_dataset
выкидывает события, произошедшие до первого целевого у 
        """
        users = self.dataset_pos.select("user_id").distinct()
        user_cnt = users.count()
        users_with_target = self.dataset_pos.filter(f.col("item_id").isin(self.target_list)) \
                                            .select("user_id").distinct()
        users_with_target_cnt = users_with_target.count()
        users_without_target = users.subtract(users_with_target)
        users_without_target_cnt = users_without_target.count()
        
        
        dataset_train_without_target = self.dataset_pos.join(users_without_target,
                                                             on="user_id",
                                                             how="inner")
        dataset_train_without_target_cnt = dataset_train_without_target.count()
        
        
        users_train, users_val_test = users_with_target.randomSplit([ 1 - self.test_size, self.test_size])
        users_train_cnt = users_train.count()
        users_val_test_cnt = users_val_test.count()
        
        
        dataset_train_with_target = self.dataset_pos.join(users_train,
                                                          on="user_id",
                                                          how="inner")
        dataset_train = dataset_train_with_target.union(dataset_train_without_target)
        
        
        users_val_test_min_target_time = self.dataset_pos.filter(f.col("item_id").isin(self.target_list)) \
                                                         .join(users_val_test,
                                                               on="user_id",
                                                               how="inner") \
                                                         .groupBy("user_id")\
                                                         .agg(f.min("timestamp").alias("min_target_timestamp"))
        dataset_val_test = self.dataset_pos.join(users_val_test_min_target_time,
                                                 on="user_id",
                                                 how="inner") \
                                    .withColumn("new_rating",
                                                f.when(f.col("timestamp") >= f.col("min_target_timestamp"), 0) \
                                                        .otherwise(f.col("rating"))) \
                                    .drop("rating", "min_target_timestamp") \
                                    .withColumnRenamed("new_rating", "rating")

        dataset_val_test_corr = dataset_val_test.filter("rating = 1")
        dataset_val_test_cnt = dataset_val_test.groupBy("user_id").count() \
                                               .select(f.avg("count").alias("avg")) \
                                               .toPandas().avg.values[0]     
        dataset_val_test_corr_cnt = dataset_val_test_corr.groupBy("user_id").count() \
                                                         .select(f.avg("count").alias("avg")) \
                                                         .toPandas().avg.values[0]
                
        dataset_val_test = dataset_val_test_corr.join(users_val_test, on="user_id", how="inner")
        dataset_full = dataset_train.union(dataset_val_test)
        dataset_full.cache().count()
        
        delta_users = self.dataset_pos.select("user_id").distinct() \
                                .subtract(dataset_full.select("user_id").distinct())
        delta_users_cnt = delta_users.count()
        
        users_val_test_corr = users_val_test.subtract(delta_users)
        
        msg_stats = ''
        msg_stats+= "user cnt: " + str(user_cnt)\
            + '\n'+ "users with target_cnt: " + str(users_with_target_cnt)\
            + '\n'+ "users without target cnt: " + str(users_without_target_cnt)\
            + '\n\n'\
            + '\n'+ 'Train user with target cnt: {}'.format(users_train_cnt)\
            + '\n' + 'Val/test user with target cnt: {}'.format(users_val_test_cnt)\
            + '\n\n'\
            + '\n' + 'Avg events before nulling: {}'.format(dataset_val_test_cnt)\
            + '\n' + 'Avg events after nulling: {}'.format(dataset_val_test_corr_cnt)\
            + '\n\n'\
            + '\n' + 'users with first target act: {}'.format(delta_users_cnt)
        self.stats = msg_stats
        
        if self.val_flg:
            users_val, users_test = users_val_test_corr.randomSplit([0.5, 0.5])
            target_val = self.dataset_target.filter(f.col("item_id").isin(self.target_list)) \
                    .join(users_val, on="user_id", how="inner")

            target_test = self.dataset_target.filter(f.col("item_id").isin(self.target_list)) \
                     .join(users_test, on="user_id", how="inner")
            return dataset_full, target_test, target_val
        
        target_test = self.dataset_target.filter(f.col("item_id").isin(self.target_list)) \
                                    .join(users_val_test_corr,
                                          on="user_id",
                                          how="inner").cache()
        
        
        return dataset_full, target_test
    
    

class Recsys360PopularModel:
    
    def __init__(
        self,
        target_list,
        col_user="user_id",
        col_item="item_id",
        col_rating="rating",
        col_prediction="prediction"
    ):
        self.target_list = target_list
        self.col_user = col_user
        self.col_item = col_item
        self.col_rating = col_rating
        self.col_prediction = col_prediction
        
        
    def fit(self, train_sdf):
        self._sdf_checker(train_sdf, [self.col_item, self.col_rating])
        
        target_item_train_sdf = train_sdf.filter(f.col(self.col_item).isin(self.target_list))
        
        target_item_pos_train_sdf = target_item_train_sdf.filter(f.col(self.col_rating) == f.lit(1))
        
        popular_items_sdf = target_item_pos_train_sdf.groupBy(f.col(self.col_item))\
                                                     .agg(f.count(f.col(self.col_item)).alias("item_cnt"))\
                                                     .orderBy(f.col("item_cnt").desc())
                
        item_cnt = int(popular_items_sdf.select(f.sum(f.col("item_cnt"))).toPandas()['sum(item_cnt)'][0])
        
        popular_sdf = popular_items_sdf.withColumn(self.col_prediction,
                                                   f.col("item_cnt") / item_cnt)\
                                       .select(f.col(self.col_item),
                                               f.col(self.col_prediction))
            
        self.popular_sdf = popular_sdf
    
    
    def predict(self, user_sdf):
        self._sdf_checker(user_sdf, [self.col_user])
        
        user_distinct_sdf = user_sdf.select(f.col(self.col_user)).distinct()
        popular_prediction = self.popular_sdf.crossJoin(user_distinct_sdf)\
                                             .select(f.col(self.col_user),
                                                     f.col(self.col_item),
                                                     f.col(self.col_prediction))
        return popular_prediction
    
    
    def _sdf_checker(self, sdf, col_list=None):
        if not isinstance(sdf, f.DataFrame):
            raise TypeError(
                "train_sdf should be Spark DataFrame"
            )
            
        sdf_columns = sdf.columns
        
        if col_list is not None:
            for col_name in col_list:
                if col_name not in sdf_columns:
                    raise TypeError(
                        "{col_name} column not in Spark DataFrame".format(col_name=col_name)
                    )
           
