from sys import api_version
from flask_apscheduler import APScheduler

from kafka import KafkaClient, KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from time import sleep
import random
import json

PROCESSO = "estoque_de_veiculos"
PROCESSO_DE_VENDA = "venda_de_veiculos"

def iniciar():
    global deslocamento
    deslocamento = 0

    cliente = KafkaClient(
        bootstrap_servers=["kafka:29092"],
        api_version=(0, 10, 1)
    )
    cliente.add_topic(PROCESSO)
    cliente.close()

ESTOQUE = "/../workdir/estoque.json"

def validar_pedido(dados_do_pedido):
    sleep(random.randint(1, 6))

    valido, titulo, mensagem = False, "", ""

    with open(ESTOQUE, "r") as arquivo_estoque:
        mensagem = "Os veiculos selecionados na venda estão disponíveis"

        estoque = json.load(arquivo_estoque)
        veiculos = estoque["veiculos"]
        for veiculo in veiculos:
            
            if veiculo["id"] == dados_do_pedido["id_veiculo"]:
                titulo = veiculo["titulo"]
                valido = True
                break

        arquivo_estoque.close()

    return valido, titulo, mensagem

def executar():
    global deslocamento

    consumidor_de_pedidos = KafkaConsumer(
        bootstrap_servers=["kafka:29092"],
        api_version=(0, 10, 1),
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000
    )
    topico = TopicPartition(PROCESSO_DE_VENDA, 0)
    consumidor_de_pedidos.assign([topico])
    consumidor_de_pedidos.seek(topico, deslocamento)

    for pedido in consumidor_de_pedidos:
        deslocamento = pedido.offset + 1

        dados_do_pedido = json.loads(pedido.value)
        valido, titulo, mensagem = validar_pedido(
            dados_do_pedido)
        if valido:
            dados_do_pedido["sucesso"] = 1
            dados_do_pedido["veiculos"] = titulo
            dados_do_pedido["mensagem"] = mensagem
        else:
            dados_do_pedido["sucesso"] = 0
        try:
            produtor = KafkaProducer(
                bootstrap_servers=["kafka:29092"],
                api_version=(0, 10, 1)
            )
            produtor.send(topic=PROCESSO, value=json.dumps(
                dados_do_pedido).encode("utf-8"))
        except KafkaError as erro:
            print(f"ocorreu um erro: {erro}")

if __name__ == "__main__":
    iniciar()

    agendador = APScheduler()
    agendador.add_job(id=PROCESSO, func=executar,
                      trigger="interval", seconds=3)
    agendador.start()

    while True:
        sleep(60)
