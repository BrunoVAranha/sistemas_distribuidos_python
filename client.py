import json
import random
import socket

from mensagem import Mensagem

# IP local padrão
ip = "127.0.0.1"
# lista que receberá as portas de cada servidor (3)
porta = []
client_sockets = []

menu_input = input("digite INIT para criar o cliente: ")
if menu_input == "INIT":

    for i in range(3):
        print('a')
        porta_input = int(input(f"Porta do servidor {i + 1}: "))
        porta.append(porta_input)

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect(('127.0.0.1', porta_input))
        client_sockets.append(client_socket)

while True:
    menu_input = input("digite PUT para inserir uma chave-valor no sistema, ou GET para receber uma chave-valor: ")
    if menu_input == "PUT":
        # Gerar numero aleatorio entre 0 e 2 para escolher entre os 3 servidores
        random_number = random.randint(0, 2)
        client_sockets[random_number].send("PUT".encode())

        key_input = input("key: ")
        value_input = input("value: ")

        # Criação da mensagem
        mensagem = Mensagem(key_input, value_input)

        # Transformar a mensagem em json para trafegar no socket
        mensagem_json = json.dumps({"key": mensagem.key, "value": mensagem.value})

        # Enviar a mensagem para o servidor sorteado
        client_sockets[random_number].send(mensagem_json.encode('utf-8'))

        # Receber o PUT_OK e timestamp
        data_timestamp = client_sockets[random_number].recv(1024).decode()
        print(
            f"PUT_OK key: {key_input} "
            f"value {value_input} "
            f"timestamp {data_timestamp} "
            f"realizada no servidor[{ip}:{porta[random_number]}]")

    elif menu_input == "GET":
        print("get")

