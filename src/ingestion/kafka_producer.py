from confluent_kafka import Producer
import json

# Configurazione base
conf = {'bootstrap.servers': "localhost:9092"} #default Kafka broker
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

if __name__ == "__main__":
    test_data = [
        {"id": 1, "event": "test_event_1", "info": "Ciao Kafka!"},
        {"id": 2, "event": "test_event_2", "info": "Funziona?"}
    ]
    print("[TEST] Tentativo di invio dati di prova...")
    push_to_kafka("gdelt_raw", test_data)
    print("[TEST] Completato.")