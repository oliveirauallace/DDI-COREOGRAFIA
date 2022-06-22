from kafka import KafkaClient, TopicPartition, KafkaConsumer
from time import sleep
import json

painel_de_preco = KafkaConsumer(
    bootstrap_servers = [ "kafka:29092" ],
    api_version = (0, 10, 1),
    auto_offset_reset = "earliest",
    consumer_timeout_ms = 1500)

topico = TopicPartition("preco_de_veiculos", 0)
painel_de_preco.assign([topico])

painel_de_preco.seek_to_beginning(topico)
while True:
    print("Aguardando retorno do preço do veículo...")

    for pedido in painel_de_preco:
        dados_do_pedido = json.loads(pedido.value)
        print(f"Dados da venda do veiculo: {dados_do_pedido}")

    sleep(4)
