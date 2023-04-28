from flask import Flask, jsonify, request
from controller import Controller
app = Flask(__name__)

@app.route('/SQLToMRTranslator/get_result',methods=['GET'])
def get_api():
    query = request.args.get('query')
    print(query)
    result = Controller(query).run()
    return result

if __name__ == '__main__':
    app.run(debug=True,host='127.0.0.1',port=8080)
