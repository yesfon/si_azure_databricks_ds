from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_date

class DeltaTableManager:
    def __init__(self, storage_path):
        """
        Inicializar gestor de tablas Delta
        :param storage_path: Ruta de almacenamiento de la tabla Delta (por ejemplo, "abfs://projectdata@sistorageacctechtest.dfs.core.windows.net/delta_table")
        """

        self.spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        self.storage_path = storage_path

    def create_table(self, schema):
        """
        Crear tabla Delta
        :param schema: Esquema de la tabla
        """
        empty_df = self.spark.createDataFrame([], schema)
        empty_df.write.format("delta").mode("overwrite").save(self.storage_path)

    def upsert_data(self, new_data, merge_key='id'):
        """
        Actualizar o insertar datos
        :param new_data: DataFrame con nuevos datos
        :param merge_key: Columna para combinar registros
        """
        delta_table = DeltaTable.forPath(self.spark, self.storage_path)
        delta_table.alias("target") \
            .merge(
                new_data.alias("source"),
                f"target.{merge_key} = source.{merge_key}"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

    def cleanup_old_data(self, retention_days=30):
        """
        Eliminar datos antiguos
        :param retention_days: Días para retener datos
        """
        delta_table = DeltaTable.forPath(self.spark, self.storage_path)
        delta_table.delete(col("fecha") < current_date() - retention_days)
