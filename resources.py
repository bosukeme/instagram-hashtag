"""
This extracts used to get hashtags for terms
"""
from flask import request
from flask_restful import Resource
import instagram_hashtag_algo
import pandas as pd
from ast import literal_eval
from schema import DataSchema

data_schema = DataSchema()

class Instagram(Resource):   

    def post(self):
        data = data_schema.load(request.files)

        bloverse_data = data['bloverse_data']

        bloverse_data_csv = pd.read_csv(bloverse_data)

        row = bloverse_data_csv.head(1)

        result = instagram_hashtag_algo.future_hashtags_entity(row)

        return {
            'Entity data': literal_eval(result)
        }



