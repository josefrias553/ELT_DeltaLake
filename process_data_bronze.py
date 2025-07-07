import pandas as pd
from build_tables import build_table_full
from config import url,endpoint_issues, params_issues, headers

def diagnostico_columnas_invalidas(df):
    print("\nTipos de datos del DataFrame:")
    print(df.dtypes)

    print("\nColumnas con tipo 'object' o tipos raros:")
    for col in df.columns:
        if df[col].dtype == 'object':
            tipos_unicos = df[col].map(type).unique()
            print(f" - {col}: tipos únicos -> {tipos_unicos}")

    print("\nColumnas 100% vacías (causan errores):")
    vacias = df.columns[df.isna().all()].tolist()
    for col in vacias:
        print(f" - {col}")

def pre_deltalake_process():
    df = build_table_full(url, endpoint_issues, params_issues, headers)

    print(f"\nTotal de filas: {len(df)}\n")

    nan_info = df.isna().sum()
    nan_info = nan_info[nan_info > 0].sort_values(ascending=False)

    if nan_info.empty:
        print("No hay columnas con valores nulos.")
    else:
        print("Columnas con valores NaN:")
        for col, cantidad in nan_info.items():
            porcentaje = (cantidad / len(df)) * 100
            print(f" - {col}: {cantidad} nulos ({porcentaje:.2f}%)")

    columnas_vacias = df.columns[df.isna().all()].tolist()
    if columnas_vacias:
        print("\nColumnas eliminadas por estar 100% vacías:")
        for col in columnas_vacias:
            print(f" - {col}")
        df = df.drop(columns=columnas_vacias)

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("")
        elif pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(0)
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col], errors="coerce").fillna(pd.NaT)
        else:
            df[col] = df[col].fillna("")

    nan_info = df.isna().sum()
    nan_info = nan_info[nan_info > 0].sort_values(ascending=False)

    if nan_info.empty:
        print("\nDespues del procesamiento no hay columnas con valores nulos. ✅✅✅\n")
    else:
        print("Todavia hay columnas con valores NaN:")
        for col, cantidad in nan_info.items():
            porcentaje = (cantidad / len(df)) * 100
            print(f" - {col}: {cantidad} nulos ({porcentaje:.2f}%)")

    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
    df["closed_at"] = pd.to_datetime(df["closed_at"], errors="coerce")

    return df