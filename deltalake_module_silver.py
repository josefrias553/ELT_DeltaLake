from deltalake import write_deltalake

def save_issues_silver(df_issues, path, partition_cols):
    try:
        print(f"Intentando guardar issues en {path}")
        if df_issues is None or df_issues.empty:
            print(f"[INFO] No hay issues para guardar en {path}.\n")
            return
        write_deltalake(path, df_issues, mode='overwrite', partition_by=partition_cols)
        print("Issues guardados en capa Silver.\n")

    except Exception as e:
        print(f"Error al guardar issues en capa Silver: {e}\n")
        raise

def save_commits_silver(df_commits, path, partition_cols):
    try:
        print(f"Intentando guardar commits en {path}")
        if df_commits is None or df_commits.empty:
            print(f"[INFO] No hay commits para guardar en {path}.\n")
            return
        write_deltalake(path, df_commits, mode='overwrite', partition_by=partition_cols)
        print("Commits guardados en capa Silver.\n")

    except Exception as e:
        print(f"Error al guardar commits en capa Silver: {e}")
        raise