"""Simple class for test purposes only on specific topic for udacity DataStreaming Nano Degreee"""
import asyncio
from confluent_kafka import Consumer

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "com.udacity.project.sfcrimes"

async def consume(topic_name):
    """Consumes data from the Kafka Topic """
    c = Consumer(
        {
            "bootstrap.servers": BROKER_URL, 
            "group.id": "sf_crimes"
        }
    )
    c.subscribe([topic_name])
    print(f"successfully subscribed to topic {topic_name}")
    curr_iteration = 0 # we use this to count ho wmany messages we have consumed
    
    while True:        
        messages = c.consume(5, timeout = 1.0) # grab 5 messages
        
        for message in messages:
            if message is None:
              continue # No data received
            
            elif message.error() is not None:
                print(f"Received an error message from the consumer: Message {message.error()}")
            
            else:
                curr_iteration += 1
                print(f"MSG ID {curr_iteration} : key: {message.key()}, value: {message.value()}")

        await asyncio.sleep(0.01)

async def call_consume(topic_name):
    """Runs the Consumer tasks"""
    t1 = asyncio.create_task(consume(topic_name))
    await t1
        
def main():
    """Start consumer for given topic name"""
    print("starting consumer for 'com.udacity.project.sfcrimes'")
    
    try:
        asyncio.run(call_consume(TOPIC_NAME))
    
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()

