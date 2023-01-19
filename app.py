import json
from flask import Flask, Markup, request, redirect, render_template, jsonify
import requests
from datetime import date
import time
import os
from flask_cors import CORS

#creates instance of app
app = Flask(__name__)
CORS(app)
app.config.from_object(__name__)

@app.route("/")
def pingroute():

    return 'OK'    

#instantiate app
if __name__ == "__main__":
    app.run(debug=True, port=3000)