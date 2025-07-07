import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError

def save_data_as_delta_full(df, path, mode="overwrite", partition_cols=None):
    if df is None or df.empty:
        print("Error, colocar el token en pipeline.conf o leer logs.")
        return
    print("Overwriting...\n")
    write_deltalake(path, df, mode=mode, partition_by=partition_cols)

def save_data_as_delta_incremental(df, path, partition_cols=None):
    if df is None or df.empty:
        print("No hay datos nuevos para guardar. Se omite la escritura incremental.\n")
        return
    try:
        new_data_pa = pa.Table.from_pandas(df)
        dt = DeltaTable(path)

        dt.merge(
            source=new_data_pa,
            source_alias="source",
            target_alias="target",
            predicate=f"target.sha = source.sha"
        ) \
            .when_matched_update_all() \
            .when_not_matched_insert_all() \
            .execute()
        print("Merge ejecutado correctamente.\n")

    except TableNotFoundError:
        print("No se encontro la tabla, creando la tabla con los datos nuevos.\n")
        write_deltalake(path, data=df, mode="overwrite", partition_by=partition_cols)

    except Exception as e:
        print(f"Error al guardar datos Delta (incremental): {e}")