"""
Author: Amanda Hanway 
Assignment: Bonus - A3: Decoupling with a Message Broker
Date: 1/22/23
Purpose: 
    This program reads in a .csv file to simulate a stream of data 
    then sends the data as a message to a queue on the RabbitMQ server every 2 seconds.

Csv data source: insurance_data.csv
https://www.kaggle.com/datasets/thedevastator/insurance-claim-analysis-demographic-and-health?resource=download

Important! We'll stream forever - or until we 
           read the end of the file. 
           Use use Ctrl-C to stop.
           (Hit Control key and c key at the same time.)
"""

import csv
import time
import pika
import sys

# ---------------------------------------------------------------------- # 
# functions

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))

        # use the connection to create a communication channel
        ch = conn.channel()

        # use the channel to declare a queue
        ch.queue_declare(queue=queue_name)

        # use the channel to publish a message to the queue
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)

        # print a message to the console for the user
        print(f" [x] Sent {message}")

    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)

    finally:
        # close the connection to the server
        conn.close()
        
# ---------------------------------------------------------------------- #
# main program entry point

if __name__ == "__main__":

    # set host and queue
    host = "localhost"
    queue_name = "hello"

    # set input file
    input_file = 'insurance_data.csv'

    # open the input file and read in a row of data
    with open(input_file, 'r') as file:
        reader = csv.reader(file, delimiter=",")      
        for row in reader:

            # use an fstring to create a message from our data
            fstring_message = f"{row}"

            # prepare a binary (1s and 0s) message to stream
            MESSAGE = fstring_message.encode()
    
            # send the message
            send_message(host, queue_name, MESSAGE)

            # sleep for a second
            time.sleep(2)






