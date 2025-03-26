from azure.identity import DefaultAzureCredential
from azure.mgmt.storage.models import BlobContainer
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.resource import ResourceManagementClient


class AzureConfigurator:
    def __init__(self, subscription_id):
        """
        Inicializar configurador de Azure

        :param subscription_id: ID de la suscripción de Azure
        """
        self.subscription_id = subscription_id

        # Credenciales por defecto (usa az login)
        self.credential = DefaultAzureCredential()

        # Clientes de gestión
        self.resource_client = ResourceManagementClient(
            self.credential,
            self.subscription_id
        )
        self.storage_client = StorageManagementClient(
            self.credential,
            self.subscription_id
        )

    def create_resource_group(self, name, location='eastus'):
        """
        Crear grupo de recursos

        :param name: Nombre del grupo de recursos
        :param location: Región de Azure
        :return: Detalles del grupo de recursos
        """
        return self.resource_client.resource_groups.create_or_update(
            name,
            {'location': location}
        )

    def create_storage_account(self, resource_group, account_name, location='eastus'):
        """
        Crear cuenta de almacenamiento

        :param resource_group: Nombre del grupo de recursos
        :param account_name: Nombre de la cuenta de almacenamiento
        :param location: Región de Azure
        :return: Cuenta de almacenamiento
        """
        return self.storage_client.storage_accounts.begin_create(
            resource_group,
            account_name,
            {
                'sku': {'name': 'Standard_LRS'},
                'kind': 'StorageV2',
                'location': location
            }
        ).result()

    def create_blob_container(self, resource_group, account_name, container_name):
        """
        Crear contenedor (blob container) en la cuenta de almacenamiento

        :param resource_group: Nombre del grupo de recursos
        :param account_name: Nombre de la cuenta de almacenamiento
        :param container_name: Nombre del contenedor
        :return: Detalles del contenedor creado
        """
        container_params = BlobContainer()  # Parámetros por defecto, se pueden agregar opciones
        return self.storage_client.blob_containers.create(
            resource_group,
            account_name,
            container_name,
            container_params
        )