"""
This extracts used to get hashtags for terms
"""
from flask import request
from flask_restful import Resource
import instagram_hashtag_algo
import pandas as pd
from ast import literal_eval
from schema import DataSchema
import json

data_schema = DataSchema()

class Instagram(Resource):   

    def post(self):
        data=instagram_hashtag_algo.pass_in_df
        
        result=instagram_hashtag_algo.future_hashtags_entity(data)

        return {
            'Entity data': literal_eval(result)
        }

