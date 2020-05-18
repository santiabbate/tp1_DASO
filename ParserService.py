import socket
import sys
import json
import time
import os
import signal

class Csv:
    def __init__(self,config):
        # Abro el archivo de configuración
        dirname = os.path.dirname(__file__)
        filename = os.path.join(dirname, config)
        with open(filename,"r") as configFile:
            # Leo el path al archivo csv 
            self.path = configFile.read()

    # Carga el archivo CSV y devuelve una lista de diccionarios con los elementos
    def toJson(self):
        keys = ['id','name','value1','value2']
        jsonList = []
        with open(self.path) as file:
            # Descarto primera línea
            file.readline()
            # Parseo cada linea del archivo
            for line in file:
                # Remuevo espacios y armo lista con elementos de cada línea
                line = line.strip('\n').split(',')
                # Creo un diccionario usando las claves (Lista `keys`) y los valores leídos del archivo (Lista `line`)
                # Y lo agrego a la lista `jsonList`
                jsonList.append(dict(zip(keys,line)))
            # Convierto a string la lista de diccionarios
            jsonOut = json.dumps(jsonList)    
        return jsonOut

class Main:
        def __init__(self, configPath):
            # Creo el objeto "csvFile", el cual toma el path al archivo correspondiente, del configPath que recibe al crearse
            self.csvFile = Csv(configPath)
        
        def main(self):
            # Inicializo manejo de SIGINT
            signal.signal(signal.SIGINT, self.sigIntHandler)

            # Creo socket UDP
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

            # Me conecto al server
            server_address = ('localhost', 10000)
            print('Connecting to {} port {}'.format(server_address[0],server_address[1]))
            self.sock.connect(server_address)

            try:
                while(1):
                    # Cargo datos del archivo csv
                    jsonString = self.csvFile.toJson()
                    
                    #Envío como bytes al socket
                    self.sock.sendall(jsonString.encode())

                    # Espero respuesta del server
                    data = self.sock.recv(1024).decode('UTF-8')
                    if data != 'OK':
                        raise Exception("Didn't receive OK form Server")
                    
                    # Intervalo de espera
                    time.sleep(30)

            # Capturo todas las excepciones
            except Exception as e:
                print(e)

            finally:
                print('\nClosing socket')
                self.sock.close()
        
        def sigIntHandler(self, sig, frame):
            exit()
            
# Punto de entrada al programa
m = Main("config.txt")
m.main()