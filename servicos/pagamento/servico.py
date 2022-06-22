from flask_apscheduler import APScheduler

from kafka import KafkaClient, KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from time import sleep
import random
import json

PROCESSO = "pagamento_de_veiculos"
PROCESSO_PRECO = "preco_de_veiculos"

def iniciar():
    global deslocamento
    deslocamento = 0

    cliente = KafkaClient(
        bootstrap_servers = [ "kafka:29092" ],
        api_version = (0, 10, 1)
    )
    cliente.add_topic(PROCESSO)
    cliente.close()

def validar_pagamento(dados_do_pagamento):
    valido, mensagem = True, ""

    sleep(random.randint(1, 6))

    if valido:
        mensagem = "Confirmação da compra do veículo autorizada mediante a confirmação de pagamento autorizada"
    else: 
        mensagem = "Confirmação da compra do veículo não autorizada mediante a confirmação de pagamento rejeitado"

    return valido, mensagem

def executar():
    global deslocamento

    consumidor_de_pagamento = KafkaConsumer(
        bootstrap_servers = [ "kafka:29092" ],
        api_version = (0, 10, 1),
        auto_offset_reset = "earliest",
        consumer_timeout_ms = 1000
    )
    topico = TopicPartition(PROCESSO_PRECO, 0)
    consumidor_de_pagamento.assign([topico])
    consumidor_de_pagamento.seek(topico, deslocamento)

    for pedido in consumidor_de_pagamento:
        deslocamento = pedido.offset + 1

        dados_do_pagamento = pedido.value
        dados_do_pagamento = json.loads(dados_do_pagamento)

        valido, mensagem = validar_pagamento(dados_do_pagamento)
        if valido:
            dados_do_pagamento["sucesso"] = 1
        else:
            dados_do_pagamento["sucesso"] = 0
        dados_do_pagamento["mensagem"] = mensagem

        try:
            produtor = KafkaProducer(
                bootstrap_servers = [ "kafka:29092" ],
                api_version = (0, 10, 1)
            )
            produtor.send(topic=PROCESSO, value= json.dumps(
                dados_do_pagamento
            ).encode("utf-8"))
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
