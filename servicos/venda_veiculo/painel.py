from kafka import TopicPartition, KafkaConsumer
from time import sleep
import json

painel_de_vendas = KafkaConsumer(
    bootstrap_servers=["kafka:29092"],
    api_version=(0, 10, 1),

    auto_offset_reset="earliest",
    consumer_timeout_ms=1000)

topico = TopicPartition("venda_de_veiculos", 0)          
painel_de_vendas.assign([topico])

painel_de_vendas.seek_to_beginning(topico)
while True:
    print("Esperando cliente para venda de veículo...")

    for pedido in painel_de_vendas:
        dados_do_pedido = json.loads(pedido.value)
        print(f"Dados da venda do veiculo: {dados_do_pedido}")

    sleep(4)