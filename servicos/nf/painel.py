from kafka import KafkaClient, TopicPartition, KafkaConsumer
from time import sleep
import json

painel_de_nf = KafkaConsumer(#altera painel_de_estoque para painel_de_nf
    bootstrap_servers = [ "kafka:29092" ],
    api_version = (0, 10, 1),
    auto_offset_reset = "earliest",
    consumer_timeout_ms = 1000)

topico = TopicPartition("nfe_de_veiculos", 0)
painel_de_nf.assign([topico])

painel_de_nf.seek_to_beginning(topico)
while True:
    print("Aguadando Nota fiscal do veiculo...")

    for pedido in painel_de_nf:
        dados_da_nf = json.loads(pedido.value)
        print(f"Dados da venda do veiculo: {dados_da_nf}")

    sleep(4)
