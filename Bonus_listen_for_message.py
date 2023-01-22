"""
Author: Amanda Hanway 
Assignment: Bonus - A3: Decoupling with a Message Broker
Date: 1/22/23
Purpose: 
    This program continously listens for messages on the queue
    and writes messages to an output file.  

Approach
---------
Simple - one producer / one consumer.

Since this process runs continuously, 
if we want to emit more messages, 
we'll need to open a new terminal window.

Terminal Reminders
-----------------
- Use Control c to close a terminal and end a process.
- Use the up arrow to get the last command executed.
"""

# add imports at the beginning of the file
import pika
import sys
import csv
import time

# define a callback function to be called when a message is received
def process_message(ch, method, properties, body):
        """ Define behavior on getting a message."""
        print(" [x] Received %r" % body.decode())

        # write to output file 
        with open(output_file_name, "a", newline='') as output_file:
            writer = csv.writer(output_file, delimiter=',')  
            writer.writerow([body.decode()])


# define a main function to run the program
def main(hn: str = "localhost"):
    """Main program entry point."""
    
    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going  
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try: 
        # use the connection to create a communication channel
        channel = connection.channel()

        # use the channel to declare a queue
        channel.queue_declare(queue="hello")

        # use the channel to consume messages from the queue
        channel.basic_consume(queue="hello", on_message_callback=process_message, auto_ack=True)

        # print a message to the console for the user
        print(" [*] Waiting for messages. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":

    # set output file 
    output_file_name = "bonus_output.txt"
    
    main("localhost")
  
     
