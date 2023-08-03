import json
import random
import socket
import time

from mensagem import Mensagem

# IP local padrão
ip = "127.0.0.1"
# lista que receberá as portas de cada servidor (3)
porta = []
client_sockets = []

# dicionario que armazena Chave-valor (key e timestamp)
keyDict = {"1": "10"}

menu_input = input("digite INIT para criar o cliente: ")
if menu_input == "INIT":
    # Se conectar aos três servidores
    for i in range(3):
        porta_input = int(input(f"Porta do servidor {i + 1}: "))
        porta.append(porta_input)
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect(('127.0.0.1', porta_input))
        client_sockets.append(client_socket)

    while True:
        menu_input = input("digite PUT para inserir uma chave-valor no sistema, ou GET para receber uma chave-valor: ")
        # Gerar numero aleatorio entre 0 e 2 para escolher entre os 3 servidores
        random_number = random.randint(0, 2)
        if menu_input == "PUT":
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
            keyDict[key_input] = data_timestamp
        elif menu_input == "GET":
            client_sockets[random_number].send("GET".encode())
            key_input = input("chave para procurar: ")
            client_sockets[random_number].send(key_input.encode())
            # Receber a mensagem (json) do servidor
            data = client_sockets[random_number].recv(1024).decode('utf-8')
            if data != "NULL":
                # deserializar json e montar tabela chave-valor
                mensagem_dict = json.loads(data)
                key = mensagem_dict["key"]
                value = mensagem_dict["value"]
                timestamp_key = mensagem_dict["timestamp"]
                server_timestamp = client_sockets[random_number].recv(1024).decode()
                # se a chave não existir no catalogo do cliente
                if key not in keyDict:
                    keyDict[key] = 0
                    if float(timestamp_key) >= float(keyDict[key]):
                        print(
                            f"GET key: {key} value: {value} obtido do servidor [{ip}:{porta[random_number]}], "
                            f"meu timestamp [{keyDict[key]}] e do servidor [{server_timestamp}]")
                        # atualizar timestamp do cliente
                        keyDict[key] = timestamp_key
                elif float(timestamp_key) >= float(keyDict[key]):
                    print(
                        f"GET key: {key} value: {value} obtido do servidor [{ip}:{porta[random_number]}], meu timestamp ["
                        f"{keyDict[key]}] e do servidor [{server_timestamp}]")
                else:
                    print("TRY_OTHER_SERVER_OR_LATER")
            else:
                print("NULL")
