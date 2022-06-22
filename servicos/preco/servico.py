from sys import api_version
from flask_apscheduler import APScheduler

from kafka import KafkaClient, KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from time import sleep
import random
import json

PROCESSO = "preco_de_veiculos"
PROCESSO_DE_VENDA = "estoque_de_veiculos"


def iniciar():
    global deslocamento
    deslocamento = 0

    cliente = KafkaClient(
        bootstrap_servers=["kafka:29092"],
        api_version=(0, 10, 1)
    )
    cliente.add_topic(PROCESSO)
    cliente.close()


PRECO = "../workdir/preco.json"


def validar_pedido(dados_do_preco):
    # simula um gasto de tempo de processamento
    sleep(random.randint(1, 6))

    valido, mensagem, preco_do_veiculo, total_compra = False, "", 0.0, 0.0

    with open(PRECO, "r") as arquivo_preco:
        

        preco = json.load(arquivo_preco)
        veiculos = preco["veiculos"]
        for veiculo in veiculos:
            
            if veiculo["id"] == dados_do_preco["id_veiculo"]:
                
                valido = True
                mensagem = "Preço do veiculo adquirido conferido com sucesso." 
                preco_do_veiculo = (veiculo["preco"])
                total_compra = dados_do_preco["quantidade"] * \
                    float(veiculo["preco"])

                break

        arquivo_preco.close()

    return valido, mensagem, preco_do_veiculo, total_compra


def executar():
    global deslocamento

    consumidor_de_preco = KafkaConsumer(
        bootstrap_servers=["kafka:29092"],
        api_version=(0, 10, 1),
        auto_offset_reset="earliest",
        consumer_timeout_ms=1500
    )
    topico = TopicPartition(PROCESSO_DE_VENDA, 0)
    consumidor_de_preco.assign([topico])
    consumidor_de_preco.seek(topico, deslocamento)

    for pedido in consumidor_de_preco:
        deslocamento = pedido.offset + 1

        dados_do_preco = json.loads(pedido.value)
        valido, mensagem, preco_do_veiculo, total_compra = validar_pedido(
            dados_do_preco)
        if valido:
            dados_do_preco["sucesso"] = 1
            dados_do_preco["Preço do Veiculo"] = preco_do_veiculo
            dados_do_preco["mensagem"] = mensagem
            dados_do_preco["Total da compra"] = total_compra
        else:
            dados_do_preco["sucesso"] = 0

        try:
            produtor = KafkaProducer(
                bootstrap_servers=["kafka:29092"],
                api_version=(0, 10, 1)
            )
            produtor.send(topic=PROCESSO, value=json.dumps(
                dados_do_preco).encode("utf-8"))
        except KafkaError as erro:
            # TODO registrar erros em log (pode ate ser um topico no kafka)
            print(f"ocorreu um erro: {erro}")


if __name__ == "__main__":
    iniciar()

    agendador = APScheduler()
    agendador.add_job(id=PROCESSO, func=executar,
                      trigger="interval", seconds=3)
    agendador.start()

    while True:
        sleep(120)
