from flask_apscheduler import APScheduler

from kafka import KafkaClient, KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from time import sleep
import faker
import random
import json

PROCESSO = "nfe_de_veiculos"
PROCESSO_PAGAMENTO = "pagamento_de_veiculos"

def iniciar():
    global deslocamento
    deslocamento = 0

    cliente = KafkaClient(
        bootstrap_servers = ["kafka:29092"],
        api_version = (0, 10, 1)
    )
    cliente.add_topic(PROCESSO)
    cliente.close()

def validar_nf(dados_da_nf):
    valido, mensagem, tipo_nota, remetente, remetente_email = True, "", "", "", ""

    sleep(random.randint(1, 6))
    dados_faker = faker.Faker(locale="pt_BR")

    num = ["2550", "2552", "2553"]
    numNFE = random.choice(num)

    if valido:
        mensagem = "A Nota Fiscal Nº " + numNFE + " foi emitida com sucesso",
        tipo_nota = "Nota de venda",
        remetente = dados_faker.name() #Utilizar biblioteca faker
        remetente_email = dados_faker.email()
    else: 
        mensagem = "Nota Fiscal não autorizada"

    return valido, mensagem, tipo_nota, remetente, remetente_email

def executar():
    global deslocamento

    consumidor_de_nf = KafkaConsumer(#retirando consumidor_de_estoque e adicionando consumidor_de_nf
        bootstrap_servers = [ "kafka:29092" ],
        api_version = (0, 10, 1),
        auto_offset_reset = "earliest",
        consumer_timeout_ms = 1000
    )
    topico = TopicPartition(PROCESSO_PAGAMENTO, 0)
    consumidor_de_nf.assign([topico])
    consumidor_de_nf.seek(topico, deslocamento)

    for pedido in consumidor_de_nf:
        deslocamento = pedido.offset + 1

        dados_da_nf = pedido.value
        dados_da_nf = json.loads(dados_da_nf)

        valido, mensagem, tipo_nota, remetente, remetente_email = validar_nf(dados_da_nf)
        if valido:
            dados_da_nf["sucesso"] = 1,
            dados_da_nf["mensagem"] = mensagem,
            dados_da_nf["Tipo NFe"] = tipo_nota,
            dados_da_nf["Remetente"] = remetente
            dados_da_nf["Enviando E-mail para"] = remetente_email
        else:
            dados_da_nf["sucesso"] = 0
        dados_da_nf["mensagem"] = mensagem

        try:
            produtor = KafkaProducer(
                bootstrap_servers = [ "kafka:29092" ],
                api_version = (0, 10, 1)
            )
            produtor.send(topic=PROCESSO, value= json.dumps(
                dados_da_nf
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
