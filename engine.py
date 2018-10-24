"""
    import needed packages
"""
import os


from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType

from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator

from time import time

from pyspark.sql.functions import lit
import logging

from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RecommendationEngine:
    """
        A movie recommendation engine Class
    """

    def __init__(self, dataset_path):
        """
            Init the recommendation engine given a Spark context and a dataset path
        """

        self.spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("Recommender-system")\
            .config("spark.driver.allowMultipleContexts", "true")\
            .getOrCreate()

        sc = self.spark.sparkContext
        sc.setLogLevel("WARN")


        logger.info("Starting up the Recommendation Engine: ")

        # Load ratings data for later use
        logger.info("Loading Ratings data...")
        ratings_file_path = os.path.join(dataset_path, 'ratings.csv')
        rating_df = self.spark.read.format("csv").option("header", "true").load(ratings_file_path).drop('timestamp').cache()

        # casting userId to int
        rating_df = rating_df.withColumn('userId', rating_df['userId'].cast(IntegerType()))
        # casting movieId to int
        rating_df = rating_df.withColumn('movieId', rating_df['movieId'].cast(IntegerType()))
        # casting rating to float
        self.rating_df = rating_df.withColumn('rating', rating_df['rating'].cast(FloatType()))

        # Load movies data for later use
        logger.info("Loading Movies data...")
        movies_file_path = os.path.join(dataset_path, 'movies.csv')
        movies_raw_df = self.spark.read.format("csv").option("header", "true").load(movies_file_path).drop('genres').cache()

        # casting movieId to int
        self.movies_df = movies_raw_df.withColumn('movieId', movies_raw_df['movieId'].cast(IntegerType()))

        # Pre-calculate movies ratings counts
        self.__count_and_average_ratings()
        self.__train_model()







    def __count_and_average_ratings(self):
        """
            This function is used to count the movie ratings and AVG for each movie
            Returns Dataframe(movieId,count,AVG(rating))
        """
        logger.info("Counting movie ratings...")
        self.movies_rating_counts_df = self.rating_df.groupby("movieId").count()
        movie_ID_with_avg_ratings_df = self.rating_df.groupby('movieId').agg({'rating': 'avg'})

        self.movies_rating_counts_df=self.movies_rating_counts_df.join(movie_ID_with_avg_ratings_df, on='movieId')


    def __train_model(self):
        """
            Train the ALS model with the current dataset(ratings_RDD) which is loaded in the init method
        """
        logger.info("Training the ALS model...")

        als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")

        param_grid = ParamGridBuilder().addGrid(
            als.rank,
            [10, 15],
        ).addGrid(
            als.maxIter,
            [10, 15],
        ).addGrid(
            als.regParam,
            [0.1, 0.01, 0.2],
        ).build()

        evaluator = RegressionEvaluator(
            metricName="rmse",
            labelCol="rating",
        )
        tvs = TrainValidationSplit(
            estimator=als,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
        )

        model = tvs.fit(self.rating_df)

        self.model = model.bestModel

        logger.info("ALS model built!")

    def __predict_ratings(self, user_and_movie_df):
        """
            This method is used to predict ratings for a given user to a movie
            Takes dataframe (user_id,movie_id)
            Returns: dataframe  with format (movieId, predicted_Rating, numRatings, AVG rating)
        """

        predicted_df = self.model.transform(user_and_movie_df)
        predicted_df= predicted_df.join(self.movies_rating_counts_df, on='movieId')

        return  predicted_df

    def add_ratings(self, ratings):
        """
            This function is used to add new user rating for the exsiting model
                it adds the new rating and then train the model with new values
            Takes Rating Object(user_id,movie_id,rating)
            return user new ratings

        """
        # Convert ratings to an RDD
        # new_ratings_RDD = self.sc.parallelize(ratings)

        # new_ratings_df = self.spark.createDataFrame(ratings, schema)
        # new_ratings_df =
        logger.info("new_ratings type(df) {} ".format(type(ratings)))
        # new_ratings = pd.DataFrame(ratings)
        # Add new ratings to the existing ones
        # self.ratings_df = self.ratings_df.union(new_ratings_df)
        # # Re-compute movie ratings count
        # self.__count_and_average_ratings()
        # # Re-train the ALS model with the new ratings
        # self.__train_model()

        return ratings

    def get_ratings_for_movie_ids(self, user_id, movie_ids):
        """
            This function is used to predicate user rating for a movie
            Takes (user_id,movie_id)
            returns Rating Object("movie_name",expected_rating,number_of_rating)
        """
# detected implicit cartesian product for LEFT OUTER join between logical plans\nProject [value#14196 AS movieId#14198]\n+- LogicalRDD [value#14196], false\nand\nProject [_2#14180 AS features#14183]\n+- Filter (UDF(1) = _1#14179)\n   +- SerializeFromObject [assertnotnull(input[0, scala.Tuple2, true])._1 AS _1#14179, staticinvoke(class org.apache.spark.sql.catalyst.expressions.UnsafeArrayData, ArrayType(FloatType,false), fromPrimitiveArray, assertnotnull(input[0, scala.Tuple2, true])._2, true, false) AS _2#14180]\n      +- ExternalRDD [obj#14178]\nJoin condition is missing or trivial.\nEither: use the CROSS JOIN syntax to allow cartesian products between these\nrelations, or: enable implicit cartesian products by setting the configuration\nvariable spark.sql.crossJoin.enabled=true;'
        requested_movies_df = self.spark.createDataFrame(movie_ids, IntegerType())

        requested_movies_df = requested_movies_df.withColumn('movieId', requested_movies_df[0].cast(IntegerType())).drop(requested_movies_df[0])
        requested_movies_df=requested_movies_df.withColumn('userId', lit(user_id)).cache()

        ratings = self.__predict_ratings(requested_movies_df)
        ratings_list = ratings.rdd.map(lambda r: (r['userId'], r['movieId'], r['prediction'], r['count'], r[4])).collect()

        return ratings_list

    def get_top_ratings(self, user_id, movies_count):
        """
            This function Recommend movies to a user it returns only movies which rated more than 25 times
            Takes (user_id,number_of_movies_needed)
            returns list of ratings('movieid, userId, predicted_rating, count, AVGrating)
        """

        unrated_movies_df = self.rating_df.filter(self.rating_df['userId'] == user_id).cache()

        unrated_movies_list = unrated_movies_df.rdd.map(lambda r: r['movieId']).collect()
        new_movies_df = self.movies_df.filter(~self.movies_df['movieId'].isin(unrated_movies_list)).withColumn('userId', lit(user_id)).drop('title').cache()

        ratings = self.__predict_ratings(new_movies_df).cache()

        ratings = ratings[(ratings['count'] >= 25)].head(movies_count)

        return ratings

