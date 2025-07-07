from build_tables import build_table_delta
from process_data_bronze import pre_deltalake_process
from preprocessing import process_commits, process_issues
from deltalake_module_silver import save_issues_silver, save_commits_silver
from config import url, endpoint_commits, headers, DELTA_ISSUES_PATH, DELTA_COMMITS_PATH, partition_options_bronze, \
    DELTA_COMMITS_SILVER_PATH, DELTA_ISSUES_SILVER_PATH, partition_options_silver
from deltalake_module_bronze import save_data_as_delta_full, save_data_as_delta_incremental

def main():
    print("Ejecutando extracción FULL...")
    df = pre_deltalake_process()
    save_data_as_delta_full(df, DELTA_ISSUES_PATH, partition_cols=partition_options_bronze[0])

    print("Ejecutando extracción INCREMENTAL...")
    df_delta = build_table_delta(url, endpoint_commits, headers)
    save_data_as_delta_incremental(df_delta, DELTA_COMMITS_PATH, partition_cols=partition_options_bronze[1])

    print("Preparando para pasar a capa silver la extraccion FULL...")
    df_silver_full = process_issues(DELTA_ISSUES_PATH)
    save_issues_silver(df_silver_full, DELTA_ISSUES_SILVER_PATH, partition_cols=partition_options_silver[0])

    print("Preparando para pasar a capa silver la extraccion INCREMENTAL...")
    df_silver_incremental = process_commits(DELTA_COMMITS_PATH)
    save_commits_silver(df_silver_incremental, DELTA_COMMITS_SILVER_PATH, partition_cols=partition_options_silver[1])

if __name__ == "__main__":
    main()