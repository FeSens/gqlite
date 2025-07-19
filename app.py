from flask import Flask, request, jsonify, render_template
from python_api import GQLiteDB

app = Flask(__name__)
db = GQLiteDB('./benchmarkdb')  # Adjust path to your DB

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/query', methods=['POST'])
def query():
    query = request.json.get('query')
    try:
        result = db.execute_query(query)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=False, port=2999)