import pickle

from flask import Blueprint
main = Blueprint('main', __name__)
 
import json
from engine import RecommendationEngine
 
import logging
logging.basicConfig(level=logging.INFO)


logger = logging.getLogger(__name__)


 
from flask import Flask, request
 
@main.route("/<int:user_id>/ratings/top/<int:count>", methods=["GET"])
def top_ratings(user_id, count):
    logger.debug("User %s TOP ratings requested", user_id)
    top_ratings = recommendation_engine.get_top_ratings(user_id,count)
    return json.dumps(top_ratings)
 
@main.route("/<int:user_id>/ratings/<int:movie_id>", methods=["GET"])
def movie_ratings(user_id, movie_id):
    logger.debug("User %s rating requested for movie %s", user_id, movie_id)
    ratings_df = recommendation_engine.get_ratings_for_movie_ids(user_id, [movie_id])
    return json.dumps(ratings_df)
    # return "True with user_id {} and movie_id {}".format(user_id,movie_id)
 
# @main.route("/<int:user_id>/ratings", methods = ["POST"])
# def add_ratings(user_id):
#     # get the ratings from the Flask POST request object
#     ratings_list1 = list(request.form.keys())[0].strip().split("\n")
#     ratings_list = map(lambda x: x.split(","), ratings_list1)
#     # create a list with the format required by the engine (user_id, movie_id, rating)
#     ratings = map(lambda x: (user_id, int(x[0]), float(x[1])), ratings_list)
#     # add them to the model using then engine API
#     recommendation_engine.add_ratings(ratings)
#     return  json.dumps(ratings_list1)


def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError


# result = json.dumps(yourdata, default=set_default)
def create_app(dataset_path):
    global recommendation_engine 

    recommendation_engine = RecommendationEngine(dataset_path)
    
    app = Flask(__name__)
    app.register_blueprint(main)
    return app 
