import os
from flask import Flask
from flask_restful import Api
from sys import platform
from resources import Instagram

if platform == 'win32':
    from dotenv import load_dotenv
    load_dotenv(".env", verbose=True)


app=Flask(__name__)
app.config.from_object("default_config")
app.config.from_envvar("APPLICATION_SETTINGS")

api=Api(app)


@app.route("/")
def home():
    return "<h1 style='color:blue'>This is the hashtags for terms pipeline!</h1>"


api.add_resource(Instagram, '/instagram')

if __name__=='__main__':
    app.run(port=5000)
