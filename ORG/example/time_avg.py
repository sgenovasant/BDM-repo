from pymongo import MongoClient

# Conectar al servidor de MongoDB (reemplaza con tu URI de conexión si es necesario)
client = MongoClient("mongodb://localhost:27017/")

db = client['Query3']

collection = db['Execution_Times']

#identificar los campos
first_doc = collection.find_one()

# Verificar
if not first_doc:
    print("La colección está vacía.")
else:
    # Identificar numéricos
    numeric_fields = [field for field in first_doc.keys() if isinstance(first_doc[field], (int, float))]

    if not numeric_fields:
        print("No encontro")
    else:
        pipeline = [
            {
                '$group': {
                    '_id': None,
                    **{field: {'$avg': f'${field}'} for field in numeric_fields}
                }
            }
        ]

        # llama pipeline
        result = list(collection.aggregate(pipeline))

        #mostrar
        if result:
            promedios = result[0]
            for field, avg_value in promedios.items():
                if field != '_id':
                    print(f"'{field}' es: {avg_value}")
        else:
            print("sin campos numéricos.")

# Cerrar la conexión
client.close()
