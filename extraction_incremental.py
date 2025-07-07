import requests, time
from dateutil import parser
from datetime import timezone

LAST_UPDATE_FILE = "last_update.txt"

def read_last_run():
    try:
        with open(LAST_UPDATE_FILE, "r") as f:
            last_run = f.read().strip()
            return parser.isoparse(last_run)
    except FileNotFoundError:
        return parser.isoparse("2023-01-01T00:00:00Z")

def save_last_run(date_time):
    with open(LAST_UPDATE_FILE, "w") as f:
        f.write(date_time.astimezone(timezone.utc).isoformat())

def get_data_delta(url_f, endpoint_f, data_field = None, params_f = None, headers_f = None, intentos=3, espera=2):
    for i in range(intentos):
        try:
            all_data = []
            url_extend = f"{url_f}/{endpoint_f}"
            while url_extend:
                response = requests.get(url_extend, params=params_f, headers=headers_f, timeout=5)
                response.raise_for_status()
                links = response.headers.get('Link', None)
                try:
                    data = response.json()
                    if data_field:
                        data = data[data_field]
                    all_data.extend(data)
                except (ValueError, KeyError, TypeError) as e:
                    print(f"Error procesando datos: {e}")
                    return None
                url_extend = None
                if links:
                    links = {rel.split('=')[1].strip('"'): url_part.strip('<>')
                             for url_part, rel in
                             (link.split(';') for link in links.split(','))}
                    url_extend = links.get('next', None)
                params_f = None
            return all_data
        except requests.exceptions.Timeout as e:
            print(f"Error al obtener datos: {e}")
            if i < intentos - 1:
                print(f"Intento {i + 1} de {intentos}")
                time.sleep(espera)
                return None
            else:
                print("No se pudo obtener los datos")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Error al obtener datos: {e}")
            return None
    return None