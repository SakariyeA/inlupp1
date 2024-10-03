import json
import random
from datetime import datetime
from azure.storage.queue import QueueServiceClient
import time

# Your Connection String from Azure Storage Account
connection_str = "DefaultEndpointsProtocol=https;AccountName=sakariyede23;AccountKey=3K4DsbpzW/q6BSB34Jvxij2q7pqJB39414Hfkvyw9BV0kAI+SNI2DJAmC1Nl2SHGb/XuDKYK/Odr+AStv0svJg==;EndpointSuffix=core.windows.net"
queue_name = "testing"  # Name of your queue

# Connect to Azure Queue
queue_service = QueueServiceClient.from_connection_string(connection_str)
queue_client = queue_service.get_queue_client(queue_name)

# Check if the queue exists by trying to fetch its properties
def check_if_queue_exists(queue_client):
    try:
        queue_client.get_queue_properties()  # Attempt to fetch the queue's properties
        print(f"Queue '{queue_name}' exists. Sending messages...")
        return True
    except Exception as e:
        print(f"Queue '{queue_name}' does not exist or could not be accessed. Error: {str(e)}")
        return False

def produce_data(max_messages=10):
    try:
        for i in range(max_messages):
            # Generate random data
            data = {
                "id": random.randint(1, 1000),  # Random ID
                "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),  # Changed format to ISO 8601
                "value": random.uniform(10, 100),  # Random value
                "description": "This is a sample message"  # Simple description for each message
            }
            # Convert to JSON format
            message = json.dumps(data)
            print(f"Sending message: {message}")
            # Send the message to the queue
            queue_client.send_message(message)
            # Pause for 2 seconds between each message to avoid overload
            time.sleep(2)

    except Exception as e:
        print(f"Error while trying to send the message: {str(e)}")

# Check if the queue exists and call the function if it does
if check_if_queue_exists(queue_client):
    produce_data(max_messages=10)