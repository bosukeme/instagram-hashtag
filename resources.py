"""
This extracts used to get hashtags for terms
"""
from flask import jsonify
from flask_restful import Resource, reqparse
import json
import instagram_hashtag_algo
import pandas as pd


class Instagram(Resource):   
    def post(self):
        bloverse_data = pd.read_csv('Bloverse Data - Articles and Entities.csv').head(1)

        return jsonify(
            instagram_hashtag_algo.future_hashtags_entity(bloverse_data)
        )



