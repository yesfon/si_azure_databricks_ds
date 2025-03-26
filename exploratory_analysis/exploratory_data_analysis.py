import os
import pandas as pd
from datetime import datetime
from sklearn.preprocessing import StandardScaler


class ExploratoryDataAnalysis:
    def __init__(self, data: pd.DataFrame, project_name: str = 'ml_project'):
        """
        Inicializa el objeto de Análisis Exploratorio de Datos.

        :param data: DataFrame de pandas con los datos a analizar
        :param project_name: Nombre del proyecto para organizar archivos de salida
        """
        self.original_data = data.copy()
        self.processed_data = data.copy()
        self.project_name = project_name
        self.output_directory = f'logs/eda_results_{project_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}'

        # Crear directorio de salida
        os.makedirs(self.output_directory, exist_ok=True)

        # Metadatos del análisis
        self.metadata = {
            'columns': list(data.columns),
            'data_types': dict(data.dtypes),
            'total_records': len(data),
            'numeric_columns': list(data.select_dtypes(include=['int64', 'float64']).columns),
            'categorical_columns': list(data.select_dtypes(include=['object', 'category']).columns)
        }

    def descriptive_analysis(self) -> dict:
        """
        Realiza un análisis descriptivo completo de los datos.

        :return: Diccionario con estadísticas descriptivas
        """
        # Estadísticas descriptivas para columnas numéricas
        numeric_statistics = self.original_data.describe().to_dict()

        # Análisis de columnas categóricas
        categorical_statistics = {}
        for col in self.metadata['categorical_columns']:
            categorical_statistics[col] = {
                'unique_values': self.original_data[col].nunique(),
                'top_5_frequency': dict(self.original_data[col].value_counts().head())
            }

        # Detección de valores nulos
        null_values = dict(self.original_data.isnull().sum())

        return {
            'numeric_statistics': numeric_statistics,
            'categorical_statistics': categorical_statistics,
            'null_values': null_values
        }

    def data_preprocessing(self,
                           handle_nulls: str = 'medium',
                           scale: bool = True,
                           encode_categorical: bool = True):
        """
        Preprocesa los datos para preparar para Machine Learning.

        :param handle_nulls: Estrategia para manejar valores nulos ('medium', 'remove')
        :param scale: Si se deben escalar las variables numéricas
        :param encode_categorical: Si se deben codificar variables categóricas
        """
        processed_data = self.original_data.copy()

        # Manejo de valores nulos
        if handle_nulls == 'medium':
            for col in processed_data.columns:
                if processed_data[col].dtype in ['int64', 'float64']:
                    processed_data[col].fillna(processed_data[col].median(), inplace=True)
                else:
                    processed_data[col].fillna(processed_data[col].mode()[0], inplace=True)
        elif handle_nulls == 'remove':
            processed_data.dropna(inplace=True)

        # Codificación de variables categóricas
        if encode_categorical:
            processed_data = pd.get_dummies(processed_data)

        # Escalamiento de variables numéricas
        if scale:
            scaler = StandardScaler()
            numeric_columns = processed_data.select_dtypes(include=['int64', 'float64']).columns
            processed_data[numeric_columns] = scaler.fit_transform(processed_data[numeric_columns])

        self.processed_data = processed_data

    def generate_report(self):
        """
        Genera un reporte completo de EDA en formato Markdown.
        """
        report_path = os.path.join(self.output_directory, 'eda_report.md')

        # Análisis descriptivo
        analysis = self.descriptive_analysis()

        with open(report_path, 'w') as f:
            f.write(f"# Reporte de Análisis Exploratorio de Datos - {self.project_name}\n\n")

            # Información general
            f.write("## Información General\n")
            f.write(f"- Total de Registros: {self.metadata['total_records']}\n")
            f.write(f"- Columnas: {', '.join(self.metadata['columns'])}\n\n")

            # Valores nulos
            f.write("## Valores Nulos\n")
            for col, nulls in analysis['null_values'].items():
                f.write(f"- {col}: {nulls} valores nulos\n")
            f.write("\n")

            # Estadísticas numéricas
            f.write("## Estadísticas Descriptivas (Variables Numéricas)\n")
            for col, stats in analysis['numeric_statistics'].items():
                f.write(f"### {col}\n")
                for stat, value in stats.items():
                    f.write(f"- {stat}: {value}\n")
                f.write("\n")

            # Estadísticas categóricas
            f.write("## Análisis de Variables Categóricas\n")
            for col, stats in analysis['categorical_statistics'].items():
                f.write(f"### {col}\n")
                f.write(f"- Valores únicos: {stats['unique_values']}\n")
                f.write("- Top 5 valores más frecuentes:\n")
                for valor, frecuencia in stats['top_5_frequency'].items():
                    f.write(f"  - {valor}: {frecuencia}\n")
                f.write("\n")

            f.write("## Visualización\n")
            # Nota: La ruta relativa puede variar según la ubicación del reporte.
            f.write("![Gráfica de EDA](logs/eda_visuals/eda_visuals.png)\n")

    def run_full_eda(self):
        """
        Ejecuta todas las etapas del análisis exploratorio.
        """
        # Preprocesar datos
        self.data_preprocessing()

        # Generar reporte
        self.generate_report()

        print(f"Análisis completado. Resultados en: {self.output_directory}")

    def get_ml_data(self):
        """
        Retorna los datos procesados listos para Machine Learning.

        :return: DataFrame con datos procesados
        """
        return self.processed_data