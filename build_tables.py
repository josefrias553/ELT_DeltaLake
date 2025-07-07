import pandas as pd
from dateutil import parser
from extraction_full import get_data_full
from extraction_incremental import get_data_delta, read_last_run, save_last_run
from datetime import timedelta

def build_table_full(url, endpoint, params, headers):
    json_data_full = get_data_full(url_f=url, endpoint_f=endpoint, params_f=params, headers_f=headers)

    try:
        df = pd.json_normalize(json_data_full)
        df["created_year"] = pd.to_datetime(df["created_at"]).dt.year

        columnas_utiles = [
        "id",
        "number",
        "title",
        "body",
        "state",
        "state_reason",
        "created_at",
        "updated_at",
        "closed_at",
        "created_year",
        "author_association",
        "user.login",
        "comments",
        "reactions.total_count",
        "reactions.+1",
        "reactions.heart",
        "timeline_url"
        ]

        return df[columnas_utiles]
    except KeyError as e:
        print(f"Columna faltante en los datos: {e}")
        return None
    except TypeError as e:
        print(f"Error al procesar los datos: {e}")
        return None
    except Exception as e:
        print(f"No se pudo crear la tabla. Error inesperado: {e}")
        return None


def build_table_delta(url, endpoint, headers):
    last_run = read_last_run()
    print(f"Extrayendo datos desde: {last_run.isoformat()}\n")

    params_delta = {"state": "all", "since": last_run}

    json_data_delta = get_data_delta(url_f=url, endpoint_f=endpoint, params_f=params_delta, headers_f=headers)

    if not json_data_delta:
        print("No se encontraron nuevos datos")
        return None

    try:
        df_f = pd.json_normalize(json_data_delta)
        max_date_str = df_f["commit.author.date"].max()
        max_date = parser.isoparse(max_date_str) + timedelta(seconds=1)
        save_last_run(max_date)
        print(f"Última fecha guardada: {max_date.isoformat()}\n")
        df_f["ingestion_date"] = pd.to_datetime("now").strftime("%Y-%m-%d")
        df_f["created_date"] = pd.to_datetime(df_f["commit.author.date"]).dt.date
        df_f.rename(columns={
            "commit.author.name": "author_name",
            "commit.author.email": "author_email",
            "commit.message": "message",
        }, inplace=True)

        columnas_utiles = [
        "sha",
        "author_name",
        "author_email",
        "committer.login",
        "message",
        "commit.comment_count",
        "commit.verification.verified",
        "ingestion_date",
        "created_date"
        ]

        return df_f[columnas_utiles]
    except KeyError as e:
        print(f"Columna faltante en los datos: {e}")
        return None
    except TypeError as e:
        print(f"Error al procesar los datos: {e}")
        return None
    except Exception as e:
        print(f"No se pudo crear la tabla. Error inesperado: {e}")
        return None