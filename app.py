from flask import Flask, request, jsonify
from threading import Lock
import time

app = Flask(__name__)
store = {}
lock = Lock()

# Helper function to check if key exists and not expired


def key_valid(key):
    if key not in store:
        return False
    if store[key]['expiry'] and time.time() > store[key]['expiry']:
        del store[key]
        return False
    return True

# SET command implementation


@app.route('/set', methods=['POST'])
def set_key():
    key = request.json['key']
    value = request.json['value']
    expiry = request.json.get('expiry', None)
    condition = request.json.get('condition', None)

    with lock:
        if condition == 'NX' and key_valid(key):
            return jsonify({'success': False, 'message': 'Key already exists.'}), 409
        elif condition == 'XX' and not key_valid(key):
            return jsonify({'success': False, 'message': 'Key does not exist.'}), 404
        else:
            store[key] = {'value': value,
                          'expiry': time.time() + expiry if expiry else None}
            return jsonify({'success': True, 'message': 'Key set successfully.'}), 200

# GET command implementation


@app.route('/get/<key>', methods=['GET'])
def get_key(key):
    with lock:
        if not key_valid(key):
            return jsonify({'success': False, 'message': 'Key not found.'}), 404
        else:
            return jsonify({'success': True, 'value': store[key]['value']}), 200


if __name__ == '__main__':
    app.run(debug=True)
