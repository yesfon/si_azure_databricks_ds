from utils import generate_daily_csvs
from data_managers.azure_configurator import AzureConfigurator
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
    downloader.download_and_process()
    generate_daily_csvs('data/raw/SuperStore_Orders.csv')


if __name__ == "__main__":
    main()
