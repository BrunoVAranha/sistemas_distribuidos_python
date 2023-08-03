import json
import socket
import threading
import time

from mensagem import Mensagem

# IP local padrão
ip = "127.0.0.1"
# dicionario que armazena key-value
key_value = {"1": ("1", "5")}
# porta do lider
porta_lider = 0
# porta deste servidor
porta_server = 0
# lista contendo a porta que o lider irá usar para se comunicar com os outros servidores
servers_port_list = []
# lista contendo os sockets conectados a outros servidores
servers_socket_list = []
# lista contendo os sockets usados para replicar mensagens aos servidores
replication_socket_list = []
# Criar um socket para aceitar conexões de clientes
server_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# Socket que conecta o servidor ao lider
lider_connect_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# variavel que indica se o servidor é lider ou não
lider = False


def handle_client(client_socket, client_address, lider_connect_socket):
    while True:
        operacao = client_socket.recv(1024).decode('utf-8')
        if operacao == "PUT":
            timestamp = time.time()
            if lider:
                # Receive JSON data from the server
                json_mensagem = client_socket.recv(1024).decode('utf-8')
                # Deserialize the JSON data to reconstruct the Mensagem object
                mensagem_dict = json.loads(json_mensagem)
                key = mensagem_dict["key"]
                value = mensagem_dict["value"]
                key_value[mensagem_dict["key"]] = (mensagem_dict["value"], timestamp)
                print(f"Cliente {ip}:{client_address[1]} PUT key:{key} value:{value}")
                # replicar a chave-valor nos servidores conectados ao lider
                mensagem = Mensagem(mensagem_dict["key"], mensagem_dict["value"], timestamp)
                mensagem_replication = json.dumps({"key": mensagem.key,
                                                   "value": mensagem.value,
                                                   "timestamp": mensagem.timestamp})
                # replicar a chave-valor para os servidores conectados ao lider
                for i in range(len(replication_socket_list)):
                    replication_socket_list[i].send("REPLICATION".encode('utf-8'))
                    replication_socket_list[i].send(mensagem_replication.encode('utf-8'))
                    # receber a confirmação da replicação
                    replication_socket_list[i].recv(1024).decode('utf-8')

                # devolver para o cliente o PUT_OK com timestamp
                print(f"Enviando PUT_OK ao Cliente {ip}]:{client_address[1]} da key:{key} ts:[{mensagem.timestamp}]")
                client_socket.send(str(timestamp).encode())
            else:
                # Direcionar o PUT ao lider
                lider_connect_socket.send("PUT".encode())
                lider_connect_socket.send(str(client_address[1]).encode('utf-8'))
                # Receber a mensagem (json) do servidor
                json_mensagem = client_socket.recv(1024)
                # deserializar json e montar tabela chave-valor
                mensagem_dict = json.loads(json_mensagem)
                key = mensagem_dict["key"]
                value = mensagem_dict["value"]
                lider_connect_socket.send(json_mensagem)
                print(f"Encaminhando PUT key:{key} value: {value}.")
                # devolver para o cliente o timestamp
                client_socket.send(str(timestamp).encode())
        elif operacao == "GET":
            key = client_socket.recv(1024).decode()
            for k, v in key_value.items():
                if k == key:
                    value, timestamp = v
                    mensagem = Mensagem(key, value, timestamp)
                    mensagem_get = json.dumps({"key": mensagem.key,
                                               "value": mensagem.value,
                                               "timestamp": mensagem.timestamp})
                    # Devolver a mensagem ao cliente
                    client_socket.send(mensagem_get.encode('utf-8'))
                    # Enviar o timestamp do servidor
                    client_socket.send(str(mensagem.timestamp).encode('utf-8'))

                    print(f"Cliente [{ip}]:[{client_address[1]}] GET key:[{mensagem.key}] ts:[{mensagem.timestamp}]. "
                          f"Meu ts é {mensagem.timestamp}], portanto"
                          f"devolvendo [{mensagem.value}].")
                    break
            # Caso nao encontre a chave, devolva NULL
            if key not in key_value:
                client_socket.send("NULL".encode('utf-8'))


def server_put_thread(server_socket):
    while True:
        data = server_socket.recv(1024).decode('utf-8')
        if data == "PUT":
            # receber porta do cliente que fez a requisicao
            client_address = server_socket.recv(1024).decode('utf-8')
            timestamp = time.time()
            # Receber a mensagem (json) do servidor
            json_mensagem = server_socket.recv(1024).decode('utf-8')
            # deserializar json e montar tabela chave-valor
            mensagem_dict = json.loads(json_mensagem)
            key_value[mensagem_dict["key"]] = (mensagem_dict["value"], timestamp)
            # replicar a chave-valor nos servidores conectados ao lider
            mensagem = Mensagem(mensagem_dict["key"], mensagem_dict["value"], timestamp)
            mensagem_replication = json.dumps({"key": mensagem.key,
                                               "value": mensagem.value,
                                               "timestamp": mensagem.timestamp})
            key = mensagem_dict["key"]
            for i in range(len(replication_socket_list)):
                replication_socket_list[i].send("REPLICATION".encode('utf-8'))
                replication_socket_list[i].send(mensagem_replication.encode('utf-8'))
                # receber a confirmação da replicação
                replication_socket_list[i].recv(1024).decode()
            # devolver para o cliente o PUT_OK com timestamp
            print(f"Enviando PUT_OK ao Cliente {ip}]:{client_address} da key:{key} ts:[timestamp_do_servidor]")


def server_replication_thread(replication_socket):
    while True:
        operacao = replication_socket.recv(1024).decode('utf-8')
        if operacao == "REPLICATION":
            # Receber a mensagem (json) do servidor
            data = replication_socket.recv(1024).decode('utf-8')
            # deserializar json e montar tabela chave-valor
            mensagem_dict = json.loads(data)
            key_value[mensagem_dict["key"]] = (mensagem_dict["value"], mensagem_dict["timestamp"])
            key = mensagem_dict["key"]
            value = mensagem_dict["value"]
            timestamp = mensagem_dict["timestamp"]
            print(f"REPLICATION key:{key} value:{value} ts:{timestamp}.")
            replication_socket.send("REPLICATION_OK".encode())


def main():
    global porta_lider, lider

    porta_input = int(input("porta deste servidor:"))
    porta_lider = int(input("porta do lider:"))

    if porta_input == porta_lider:
        lider = True
        # Criar um socket para aceitar conexões dos outros servidores
        server_lider_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_lider_socket.bind((ip, (porta_lider - 1)))
        server_lider_socket.listen(2)
        # Para cada servidor conectado ao lider:
        for i in range(2):
            # Aceitar conexão com um servidor e adicionar o socket criado a uma lista
            server_socket, server_address = server_lider_socket.accept()
            servers_socket_list.append(server_socket)
            servers_port_list.append(server_address[1])

            lider_replication_socket, replication_address = server_lider_socket.accept()
            replication_socket_list.append(lider_replication_socket)

            # Criar uma thread que cria um socket para ouvir as requisições PUT
            server_put = threading.Thread(target=server_put_thread, args=(server_socket,))
            server_put.start()

    else:
        # se conectar ao lider
        lider_connect_socket.connect((ip, (porta_lider - 1)))

        replication_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        replication_socket.connect((ip, (porta_lider - 1)))
        # Criar uma thread que cria um socket para receber as REPLICATIONS do lider
        server_replication = threading.Thread(target=server_replication_thread, args=(replication_socket,))
        server_replication.start()

    try:
        server_client_socket.bind((ip, porta_input))
        server_client_socket.listen(4)

        while True:
            # Aceitar conexão com cliente
            client_socket, client_address = server_client_socket.accept()
            # Criar thread que processa as requisições dos clientes
            client_thread = threading.Thread(target=handle_client, args=(client_socket,
                                                                         client_address,
                                                                         lider_connect_socket))
            client_thread.start()

    except KeyboardInterrupt:
        server_client_socket.close()


if __name__ == "__main__":
    main()
