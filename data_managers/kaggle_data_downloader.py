import os
import kaggle
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential


class KaggleDataDownloader:
    def __init__(self, dataset_name, storage_account_name, container_name):
        """
        Inicializar downloader de Kaggle con subida a Azure

        :param dataset_name: Nombre del dataset en Kaggle
        :param storage_account_name: Nombre de la cuenta de Azure Storage
        :param container_name: Nombre del contenedor
        """
        # Configuración de Kaggle
        self.dataset_name = dataset_name

        # Configuración de Azure Storage
        credential = DefaultAzureCredential()
        self.blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account_name}.blob.core.windows.net",
            credential=credential
        )
        self.container_client = self.blob_service_client.get_container_client(container_name)

    def download_and_process(self, local_path='./data/raw'):
        """
        Descargar dataset de Kaggle y procesar

        :param local_path: Ruta local para guardar datos
        :return: Ruta de archivos procesados
        """
        # Crear directorio si no existe
        os.makedirs(local_path, exist_ok=True)

        try:
            # Descargar dataset de Kaggle
            kaggle.api.dataset_download_files(
                self.dataset_name,
                path=local_path,
                unzip=True
            )

            # Procesar archivos
            processed_files = []
            for filename in os.listdir(local_path):
                if filename.endswith('.csv'):
                    file_path = os.path.join(local_path, filename)

                    # Leer y procesar
                    df = pd.read_csv(file_path, encoding='latin-1')

                    # Subir a Azure Blob Storage
                    with open(file_path, "rb") as data:
                        blob_name = f"raw/{filename}"
                        self.container_client.upload_blob(
                            name=blob_name,
                            data=data,
                            overwrite=True
                        )

                    processed_files.append(file_path)

            return processed_files

        except Exception as e:
            print(f"Error en descarga: {e}")
            return []