from flask import Flask, jsonify, request
from models import Task
import data_store

app = Flask(__name__)

def validate_task_data(data):
    return all(key in data for key in ("title", "description"))

@app.route('/tasks', methods=['POST'])
def create_task():
    data = request.json

    if not data or not validate_task_data(data):
        return jsonify({"error": "Invalid task data"}), 400
    
    new_task = Task(
        id=data_store.get_next_id(),
        title=data['title'],
        description=data['description'],
        completed=data.get('completed', False)
    )

    data_store.add_task(new_task)
    return jsonify(new_task.to_dict()), 201

@app.route('/tasks', methods=['GET'])
def get_tasks():
    tasks_from_store = data_store.get_all_tasks()

    tasks_list_dict = [task.to_dict() for task in tasks_from_store]

    return jsonify(tasks_list_dict)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)