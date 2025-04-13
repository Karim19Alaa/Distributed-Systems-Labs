from flask import Flask, request, render_template, jsonify, send_from_directory
from flask_socketio import SocketIO, emit, join_room
import logging
import time
import os
import json
from collections import defaultdict

# Configure simple logger
def setup_logger():
    logger = logging.getLogger('update_server')
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(levelname)s:%(name)s:%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

# Initialize Flask and SocketIO
app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')
logger = setup_logger()

# Data stores for different views
action_counts = defaultdict(int)  # For business view
ad_type_counts = defaultdict(int)  # For ads view
client_activity = defaultdict(lambda: defaultdict(int))  # For client view

# Track statistics about updates
stats = {
    'start_time': time.time(),
    'updates_by_service': {'business': 0, 'ads': 0, 'client': 0},
    'total_updates': 0
}

@app.route('/static/<path:path>')
def send_static(path):
    return send_from_directory('static', path)

@app.route('/update/<service>', methods=['POST'])
def update(service):
    """Endpoint to receive update messages and broadcast them to connected clients."""
    logger.info(f"Received update for service: {service}")
    
    # Get the data from the request
    data = request.get_json()
    if not data:
        logger.warning(f"Received empty update for service: {service}")
        return jsonify({'status': 'error', 'message': 'No data provided'}), 400
    
    # Update statistics
    stats['updates_by_service'][service] = stats['updates_by_service'].get(service, 0) + 1
    stats['total_updates'] += 1
    
    # Add timestamp if not present
    if 'timestamp' not in data:
        data['timestamp'] = time.time()
    
    # Process data based on service type
    if service == 'business' and 'action' in data:
        action_counts[data['action']] += 1
        # Update the data with current counts for all actions
        data['action_counts'] = dict(action_counts)
    
    elif service == 'ads' and 'ad_type' in data:
        ad_type_counts[data['ad_type']] += 1
        # Update the data with current counts for all ad types
        data['ad_type_counts'] = dict(ad_type_counts)
    
    elif service == 'client' and 'user_id' in data and 'action' in data:
        client_activity[data['user_id']][data['action']] += 1
        # Update the data with current activity for this client
        data['client_activity'] = {
            data['user_id']: dict(client_activity[data['user_id']])
        }
        data['all_client_activity'] = {
            user: dict(activities) 
            for user, activities in client_activity.items()
        }
    
    # Send to clients via Socket.IO
    socketio.emit('update', data, room=service)
    logger.info(f"Broadcasted update to {service} room")
    
    return jsonify({
        'status': 'success', 
        'message': f'Update sent to {service}'
    })

@app.route('/')
def index():
    """Root endpoint - redirects to business page."""
    logger.info("Root page accessed, redirecting to dashboard")
    return render_template('index.html')

@app.route('/business')
def business_page():
    """Business updates page showing action histogram."""
    logger.info("Business page accessed")
    return render_template('business.html', action_counts=action_counts)

@app.route('/ads')
def ads_page():
    """Ads updates page showing ad type histogram."""
    logger.info("Ads page accessed")
    return render_template('ads.html', ad_type_counts=ad_type_counts)

@app.route('/client')
def client_page():
    """Client updates page showing client activity histogram."""
    logger.info("Client page accessed")
    return render_template('client.html', client_activity=client_activity)

@app.route('/api/data/business')
def business_data():
    """API endpoint to get current business data."""
    return jsonify({
        'action_counts': dict(action_counts)
    })

@app.route('/api/data/ads')
def ads_data():
    """API endpoint to get current ads data."""
    return jsonify({
        'ad_type_counts': dict(ad_type_counts)
    })

@app.route('/api/data/client')
def client_data():
    """API endpoint to get current client data."""
    return jsonify({
        'client_activity': {
            user: dict(activities) 
            for user, activities in client_activity.items()
        }
    })

@app.route('/stats')
def server_stats():
    """Returns server statistics."""
    uptime = time.time() - stats['start_time']
    return jsonify({
        'uptime': uptime,
        'uptime_formatted': f"{int(uptime // 3600)}h {int((uptime % 3600) // 60)}m {int(uptime % 60)}s",
        'total_updates': stats['total_updates'],
        'updates_by_service': stats['updates_by_service']
    })

@socketio.on('join')
def on_join(data):
    """Handler for when a client joins a specific service room."""
    if 'service' not in data:
        logger.warning("Join attempt without service specification")
        return
    
    service = data['service']
    logger.info(f"Client joined {service} room")
    
    join_room(service)
    
    # Send initial data based on service type
    if service == 'business':
        emit('update', {
            'message': f'Connected to {service} updates.',
            'timestamp': time.time(),
            'type': 'connection',
            'action_counts': dict(action_counts)
        }, room=service)
    elif service == 'ads':
        emit('update', {
            'message': f'Connected to {service} updates.',
            'timestamp': time.time(),
            'type': 'connection',
            'ad_type_counts': dict(ad_type_counts)
        }, room=service)
    elif service == 'client':
        emit('update', {
            'message': f'Connected to {service} updates.',
            'timestamp': time.time(),
            'type': 'connection',
            'client_activity': {
                user: dict(activities) 
                for user, activities in client_activity.items()
            }
        }, room=service)
    else:
        emit('update', {
            'message': f'Connected to {service} updates.',
            'timestamp': time.time(),
            'type': 'connection'
        }, room=service)

@socketio.on('connect')
def on_connect():
    """Handler for when a client connects."""
    logger.info(f"Client connected: {request.sid}")

@socketio.on('disconnect')
def on_disconnect():
    """Handler for when a client disconnects."""
    logger.info(f"Client disconnected: {request.sid}")

if __name__ == '__main__':
    host = os.environ.get('HOST', '0.0.0.0')
    port = int(os.environ.get('PORT', 5000))
    
    logger.info(f"Starting update server on {host}:{port}")
    socketio.run(app, host=host, port=port)