import pymongo
import time
from pymongo.read_preferences import ReadPreference


uri = "mongodb://mongo1:30001,mongo2:30002,mongo3:30003/?replicaSet=my-replica-set"
primario_actual = None 

print("Conectando al clúster...")
try:
    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.server_info() 
    print("¡Conexión exitosa al Replica Set!")
    primario_actual = client.primary
    print(f"El nodo primario actual es: {primario_actual}")

except Exception as e:
    print(f"Error de conexión: {e}")
    print("¡Verifica que modificaste el archivo /etc/hosts!")
    exit() 


# consistencia
print("\n--- Iniciando Prueba 4.a (Consistencia) ---")
try:
    
    db = client["Politica"] 
    coleccion = db["Discursos"] 
    
    # 1. Escribimos en el Primario (comportamiento por defecto)
    print("Escribiendo documento de prueba en el Primario...")
    coleccion.update_one(
        {"_id": "prueba_consistencia"},
        {"$set": {"texto": "hola mundo", "timestamp": time.time()}},
        upsert=True # Si ya existe, lo actualiza. Si no, lo crea.
    )
    
    # 2. Leemos desde un Secundario
    print("Esperando 1s para asegurar la replicación...")
    time.sleep(1)
    
    # Creamos una referencia a la colección que PREFIERE leer de secundarios
    coleccion_secundaria = db.get_collection(
        "Discursos",
        read_preference=ReadPreference.SECONDARY_PREFERRED
    )
    
    doc = coleccion_secundaria.find_one({"_id": "prueba_consistencia"})
    
    if doc:
        print("¡ÉXITO! Documento encontrado en un Secundario:")
        print(doc)
    else:
        print("FALLO: No se encontró el documento en el Secundario.")

except Exception as e:
    print(f"Error durante la prueba 4.a: {e}")

