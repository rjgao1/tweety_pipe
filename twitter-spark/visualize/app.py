import ast # ast is used for parsing json files
from flask import Flask,jsonify,request # Flask: create app basic configuration, jsonify: format transformation request: send requests
from flask import render_template # convert processed data into html

# create flask app
app = Flask(__name__)
words = [] # store lables for visualization
counts = [] # store values for visualization

# implement /updateData to parse data sent from spark and return correct status code according to request form
# when using 'POST' to query path/updateData, we will use update_data_from_spark function
@app.route('/updateData', methods=['POST']) # Default methods = ['GET']
def update_data_from_spark():
    global words, counts # access global vars
    if not request.form or 'words' not in request.form: # check if request form has words field 
        return "error", 400
    words = ast.literal_eval(request.form['words']) # use ast to parse words field
    counts = ast.literal_eval(request.form['counts']) # use ast to parse counts field
    print("current words: " + str(words))
    print("current counts: " + str(counts))
    return"success", 201

# implement /updateChart to refresh and update hashtag labels and counts
@app.route('/updateChart')
def refresh_hashtag_data():
    global words, counts
    print("current words: " + str(words))
    print("current data: " + str(counts))
    return jsonify(sWords=words, sCounts=counts) # jsonify so JS code can parse the data

# home page: create functions to initialize hashtag page, 
# show_chart function binds to homepage, and it uses global labels and values to show the chart.
@app.route("/")
def showChart():
    global words,counts
    counts = []
    words = []
    return render_template('chart.html', counts=counts, words=words) # template is jinja, which embeds data into html for visualization

# main method
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050)


