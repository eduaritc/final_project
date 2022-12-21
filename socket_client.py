import socket
import pickle
import pandas as pd


def client_program():
    host = socket.gethostname()  # as both code is running on same pc
    port = 5000  # socket server port number

    client_socket = socket.socket()  # instantiate
    client_socket.connect((host, port))  # connect to the server

    message = input(" -> ")  # take input
    while message.lower().strip() != 'bye':
        data = b""
        client_socket.send(message.encode())  # send message
        packet = client_socket.recv(1024)
        while len(packet) == 1024:
            data += packet
            packet = client_socket.recv(1024)
        data+=packet
        full_message = pickle.loads(data, encoding="UTF-16")
        print('Received from server: \n' + full_message)  # show in terminal
        full_message_df = pd.DataFrame(full_message)
        full_message_df.to_csv("reviews_socketed.csv")
        message = input(" -> ")  # again take input
    client_socket.close()  # close the connection

client_program()