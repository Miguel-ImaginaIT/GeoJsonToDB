import os
import json
import psycopg2
import argparse
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

#KEYS TO EXTRACT
luminaries_data_to_extract = ["tipo_bloq_","unidad_lum","centro_man","rotulacion","tipo_bloqu",
                              "tipo_uso_u","comentario","comentar_b","zona_singu","rotation","enabled",
                              "numero","unidadlumi","uluminosa_","tramo_alum","tramo_al_1",
                              "modificado","lote","entrega","point_x","point_y","codbar","coddis",
                              "cod_zsingu","codndp","fecha_modi","obra_asoci","cod_via","entregado",
                              "rotqg","ruta_svg","nivel","id","objectid_1","GlobalID","CreationDa",
                              "Creator","EditDate","Editor","fecha_edic","FechaAltaO","FechaBajaO",
                              "fecha_alta","fecha_baja","IdLote3","Salida"]
circuits_json_path = []

# Define your table name and schema
table_name = 'geojson_data'
table_schema = """
    CREATE TABLE geojson_data (
        id SERIAL PRIMARY KEY,
        CM varchar(50),
        type VARCHAR(50),
        value json
    );
    """

def create_or_update_table():
    # Conexión a la base de datos
    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
    cursor = conn.cursor()
    with cursor:
        # Check if the table exists
        cursor.execute(sql.SQL("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
        """), [table_name])
        
        exists = cursor.fetchone()[0]
        
        # Create the table if it doesn't exist
        if not exists:
            cursor.execute(sql.SQL(table_schema))
            conn.commit()
            print(f"Table '{table_name}' has been created.")
        else:
            print(f"Table '{table_name}' already exists.")

def open_db_connection():
    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
    return conn

def close_db_connection(conn):
    if conn is not None:
        conn.close()

def delete_registry_if_exists(conn, CM, _type):
    query = """
    DELETE FROM geojson_data
    WHERE CM = %s and type = %s
    RETURNING *;
    """
    
    cursor = conn.cursor()
    try:
        cursor.execute(query, (CM,_type,))  # Pass the condition as a parameter
        deleted_record = cursor.fetchone()  # Fetch the deleted record if it exists
        conn.commit()
        
        if deleted_record:
            print("Deleted record")
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error executing query in delete method: {error}")
    
    finally:
        cursor.close()

def insert_json(conn,_type, jsondata):
    query = ""
    cursor = None
    try:
        CM = jsondata["name"].get('value')
        
        #check if exists
        delete_registry_if_exists(conn,CM,_type)

        # Get cursor
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        query = f"""INSERT INTO geojson_data (CM, type, value) VALUES (%s,%s,%s)"""
        # Execute the insert query
        cursor.execute(query, (CM,_type,json.dumps(jsondata),))
        # Confirmar la transacción
        conn.commit()

        return True

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error executing query in insert method: {query} - Exception: {error}")
        return False

    finally:
        # Close connection
        if cursor:
            cursor.close()

def append_to_json_string(json_data, new_data):
    # Parse the JSON string into a Python data structure
    data = json_data
    
    # Check if the data is a list or dictionary
    if isinstance(data, list):
        # If the data is a list, append the new data
        data.append(new_data)
    elif isinstance(data, dict):
        # If the data is a dictionary, update the dictionary
        data.update(new_data)
    else:
        raise ValueError("JSON string must be a list or dictionary")
    
    # Convert the data structure back into a JSON string
    return data

def transform_to_json(name, _type, features_data):
    # Define the base structure
    data = {
        "name": {
            "type": "Text",
            "value": name,
            "metadata": {}
        },
        "crs": {
            "type": "StructuredValue",
            "value": {
                "type": "name",
                "properties": {
                    "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
                }
            },
            "metadata": {}
        },
        "features": {
            "type": "StructuredValue",
            "value": [],
            "metadata": {}
        }
    }
    for item in features_data:    
        feature_entry = {
                "type": "Feature",
                "properties": item['properties'],
                "geometry": {
                    "type": "Point" if _type == "LUM" else "MultiLineString" "",
                    "coordinates": item['coordinates']
                }
            }
        data["features"]["value"].append(feature_entry)

    return data

def get_properties_except(jsondata, exclude_key):
    return {key: value for key, value in jsondata.items() if key != exclude_key}

# Función para procesar archivos GeoJSON
def store_geojson(folder_path):
    _luminaries_json = []
    _circuits_json = []
    
    for _file in os.listdir(folder_path):
        if _file.endswith('.geojson'):
            _file_path = os.path.join(folder_path, _file)
            
            with open(_file_path) as f:
                data = json.load(f)
                CM = data["name"][:data["name"].find('_')]
                _type = "LUM"
                _extracted_features = []
                for feature in data['features']:
                    _extracted_data = {'coordinates':feature['geometry']['coordinates'],
                                        'properties': get_properties_except(feature['properties'],'tipo_secci')
                                    }

                    
                    _extracted_features.append(_extracted_data)

                if "lum" in _file.lower():
                    print("Luminaries for CM: ",CM)
                    _type = "LUM"
                    _luminaries_json.append(transform_to_json(CM,_type,_extracted_features))

                elif "circuit" in _file.lower() or "tramos" in _file.lower():
                    print("File for circuit")
                    _type = "CIR"
                    _circuits_json.append(transform_to_json(CM,_type,_extracted_features))
                else:
                    print(f"{_file} not match with any pattern to be processed.")
                    continue

    if _luminaries_json != None and len(_luminaries_json):
        print(f"Saving luminaries geojson. Number of luminaries json {len(_luminaries_json)}")
        conn = open_db_connection()
        for item in _luminaries_json:
            insert_json(conn,"LUM",item)
        close_db_connection(conn)

    if _circuits_json != None and len(_circuits_json):
        print(f"Saving circuits geojson. Number of circuits json {len(_circuits_json)}")
        conn = open_db_connection()
        for item in _circuits_json:
            insert_json(conn,"CIR",item)
        close_db_connection(conn)


def main(_db_host, _db_name, _db_user, _db_pass, _folder_process):
    global DB_HOST, DB_NAME, DB_USER, DB_PASS  # Declare as global
    DB_HOST = _db_host
    DB_NAME = _db_name
    DB_USER = _db_user
    DB_PASS = _db_pass

    # Create table for json
    create_or_update_table()
    # Process data.
    store_geojson(_folder_process)

if __name__ == "__main__":
    # Crear el parser de argumentos
    parser = argparse.ArgumentParser(description="Script to process the geojson to store in Postgres.")
    
    # Definir los argumentos que se aceptarán
    parser.add_argument('--db_host', type=str, required=True, help='Database host')
    parser.add_argument('--db_name', type=str, required=True, help='Databaes name')
    parser.add_argument('--db_user', type=str, required=True, help='Database username')
    parser.add_argument('--db_pass', type=str, required=True, help='Database password')
    parser.add_argument('--folder', type=str, required=True, help='Folder to process')

    # Parsear los argumentos
    args = parser.parse_args()

    # Llamar a la función main con los argumentos
    main(args.db_host, args.db_name, args.db_user, args.db_pass, args.folder)