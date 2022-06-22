from kafka import KafkaClient, TopicPartition, KafkaConsumer
from time import sleep
import json

painel_de_pagamento = KafkaConsumer(
    bootstrap_servers = [ "kafka:29092" ],
    api_version = (0, 10, 1),
    auto_offset_reset = "earliest",
    consumer_timeout_ms = 1000)

topico = TopicPartition("pagamento_de_veiculos", 0)
painel_de_pagamento.assign([topico])

painel_de_pagamento.seek_to_beginning(topico)
while True:
    print("Aguadando pagamento do veiculo...")

    for pedido in painel_de_pagamento:
        dados_do_pagamento = json.loads(pedido.value)
        print(f"Dados da venda do veiculo: {dados_do_pagamento}")

    sleep(4)
