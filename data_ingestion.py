from data_managers.azure_configurator import AzureConfigurator
from data_managers.delta_table_manager import DeltaTableManager
from data_managers.kaggle_data_downloader import KaggleDataDownloader

def main():
    # Configuración de Azure
    SUBSCRIPTION_ID = '7b25ccbb-ab89-4ef1-b64c-9ae218e1d7f8'
    RESOURCE_GROUP = 'si_data_ingestion'
    STORAGE_ACCOUNT = 'sistorageacctechtest'
    CONTAINER_NAME = 'projectdata'

    # Configurar recursos de Azure
    azure_config = AzureConfigurator(SUBSCRIPTION_ID)

    # Crear grupo de recursos (si no existe)
    azure_config.create_resource_group(RESOURCE_GROUP)

    # Crear cuenta de almacenamiento (si no existe)
    storage_account = azure_config.create_storage_account(RESOURCE_GROUP, STORAGE_ACCOUNT)

    # Crear contenedor de Blob Storage (si no existe)
    container = azure_config.create_blob_container(RESOURCE_GROUP, STORAGE_ACCOUNT, CONTAINER_NAME)

    # Descargar datos de Kaggle y subirlos a Blob Storage
    downloader = KaggleDataDownloader(
        dataset_name='aditisaxena20/superstore-sales-dataset',
        storage_account_name=STORAGE_ACCOUNT,
        container_name=CONTAINER_NAME
    )
    downloaded_files = downloader.download_and_process()

    # Configurar tabla Delta
    DELTA_STORAGE_PATH = f"abfs://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/delta_table"
    delta_manager = DeltaTableManager(DELTA_STORAGE_PATH)

    # Crear tabla con el esquema adecuado
    from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("fecha", DateType(), False),
        StructField("valor", DoubleType(), True)
    ])

    delta_manager.create_table(schema)
    print("Tabla Delta creada en:", DELTA_STORAGE_PATH)

if __name__ == "__main__":
    main()
