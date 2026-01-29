# Streaming Data Analytics Assignment 1 

This repository explains the steps taken to create the cafe event pipeline for INF2214 Assignment 1. 

## Prerequisites

Required Python packages for this assignment is located in the requirements.txt file. 

This assignment uses Docker as the container application for Kafka streams.

## Repository Files

`requirements.txt` - Contains the required libraries for the project to run.

`cafe_event_schema.JSON` - JSON file that defines variable formats.

`docker-compose.yml` - Defines the Kafka streams infrastructure.

`producer.py` - Contains the message that will be sent to the demo topic.

`consumer.py` - Connects to the Kafka broker and subscribes to the demo topic to listen for events.

## Running the pipeline 

1. Initialize docker 
2. Create the demo topic 
3. Run producer.py 
4. Run consumer.py 

## Steps for creating the pipeline

### Initializing Docker 

To initialize Docker I first created a docker-compose.yml file. 

I assigned the container the default port 9092, and the schema was assigned port 8081. 

Per the assignment instructions the replication factor was set to '1'. 

I ran the following code in the terminal to initialize the Docker container:

```docker compose up -d ```

To create the 'demo' topic and set 3 partitions in the stream, I ran the following code:

```
kafka-topics --create \
  --topic demo \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
```

### Defining the event schema 

The event schema is defined in the cafe_event_schema.JSON file. 

To create the event, I first specified the schema as draft 7 of the JSON schema. 

I then listed the 7 objects in the assignment instructions as required. 

I then gave the defined the properties for each of the 7 objects including their type, and minimum values. 

### Creating the producer event

The producer event is created in the producer.py Python file. 

The producer file first loads in required libraries. 
Once the libraries are loaded, the producer event connects to the schema file and then to the Kafka server. 
Per the assignment instructions, the ack is set to 1 so that the partition leader sends the message acknowledgement back to the producer. 
The client.id is set to "Marco's Awesome Cafe" so that Kafka knows which cafe the messages are being produced from.

Once connected, the file creates the events. 
I created the events by first creating a list of customer names and drink names that the randomizer can pick from. 
3 variables are randomly generated with constraints. 
The quantity of drinks can be between 1 an 5. 
The purchase amount can be between 2.0 and 20.0 rounded to the nearest cent. 
Membership status can be true or false. 
The event id is generated in ascending, chronological order.
The timestamp is generated the time of event creation. 

For the purpose of this assignment, I set the key assignment to `None` so that the events are randomly assigned to the 3 created partitions. 

### Creating the consumer listener

The consumer is created in the consumer.py Python file. 

The consumer file first loads the required libraries.
Once the libraries are loaded I pointed the file to read the same JSON schema file as the producer located at localhost 8081. 

After the schema is read, I configured the consumer settings.
I pointed the Kafka port to the default 9092 which is the same as the port the Producer is sending messages to. 
I set the group.id to "demo-consumer" to indicate that this consumer is part of the initial stream setup and not a real-world consumer.
I set the auto.offset.reset to "earliest" so that the consumer will read all 5 messages again if an error happens so that I can print out all 5 messages for the purpose of this quiz. 
I set the enable.auto.commit value to true since the default value is typically 5 seconds which is more than enough time to consume the 5 messages for this assignment. Additionally, since the messages are sent manually when I run the Producer.py code, there's less of a need to set manual commits. 

After the configuration I tell the Consumer to subscribe to the demo topic to listen for messages there. 

Once subscribed the Consumer.py file continuously listens for messages and waits 1 second to determien if there is a new message. If no message is received, the consumer keeps listening. If a message is received in the proper JSON schema, the consuemr prints the corresponding message. 