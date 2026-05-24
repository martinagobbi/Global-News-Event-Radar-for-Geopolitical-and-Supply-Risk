from confluent_kafka import Producer
import json
import os

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
# Configurazione base
conf = {'bootstrap.servers': KAFKA_SERVER}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Errore: {err}")

def push_to_kafka(topic, data_list):
    """
    Prende una lista di dizionari (i record GDELT) 
    e li manda su Kafka.
    """
    for record in data_list:
        # Convertiamo il record in stringa JSON
        message = json.dumps(record).encode('utf-8')
        
        # Invio asincrono
        producer.produce(topic, value=message, callback=delivery_report) #callback per gestire errori di invio
    
    # Aspetta che tutti i messaggi siano inviati
    producer.flush()

    # In fondo a kafka_producer.py
if __name__ == "__main__":
    test_events = [
        {"GlobalEventID": 11111, "EventCode": "020", "Country": "IT", "info": "Test Evento"}
    ]
    test_mentions = [
        {"GlobalEventID": 11111, "MentionDocTone": 5.4, "info": "Test Menzione"}
    ]
    
    print(f"[TEST] Tentativo di invio dati di prova a {KAFKA_SERVER}...")
    
    try:
        push_to_kafka("gdelt_events_raw", test_events)
        push_to_kafka("gdelt_mentions_raw", test_mentions)
        print("[TEST] Completato con successo su entrambi i nuovi topic!")
    except Exception as e:
        print(f"[TEST ERRORE] Qualcosa è andato storto: {e}")