from flask import Flask
from app.rout.psql_routs import stats_blueprint
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
app.register_blueprint(stats_blueprint,url_prefix='/sql_stats')

if __name__ == "__main__":
    print("Starting SQL Flask Server")
    app.run(debug=True,port=5001)
