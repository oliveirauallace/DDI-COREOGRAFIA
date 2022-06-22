from kafka import KafkaClient, TopicPartition, KafkaConsumer
from time import sleep
import json

painel_de_estoque = KafkaConsumer(
    bootstrap_servers = [ "kafka:29092" ],
    api_version = (0, 10, 1),
    auto_offset_reset = "earliest",
    consumer_timeout_ms = 1000)

topico = TopicPartition("estoque_de_veiculos", 0)
painel_de_estoque.assign([topico])

painel_de_estoque.seek_to_beginning(topico)
while True:
    print("Aguardando retorno de disponibilidade do veiculo...")

    for pedido in painel_de_estoque:
        dados_do_pedido = json.loads(pedido.value)
        print(f"Dados da venda do veiculo: {dados_do_pedido}")

    sleep(4)
