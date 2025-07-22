from flask import Flask, render_template, jsonify, request
import screener

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/run_screener', methods=['POST'])
def run_screener():
    stocks = screener.get_matching_contracts()
    return jsonify(stocks)

if __name__ == '__main__':
    app.run(debug=True)
