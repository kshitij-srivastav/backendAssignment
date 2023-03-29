from flask import Flask, request, jsonify
import random

app = Flask(__name__)


@app.route('/', methods=['POST'])
def add_random_number():
    request_data = request.get_json()
    original_data = request_data['data']
    random_number = random.randint(100, 999)
    modified_data = original_data + str(random_number)
    response_data = {'result': modified_data}
    return jsonify(response_data)


if __name__ == '__main__':
    app.run(debug=True)
