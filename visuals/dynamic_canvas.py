import os
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt


class DynamicCanvas:
    def __init__(self, data):
        """
        Inicializa el canvas dinámico de visualización.

        :param data: DataFrame de pandas
        """
        self.data = data
        self.plots = []
        self.fig = None
        self.axs = None

    def _calculate_grid(self, num_plots):
        """
        Calcula la disposición óptima de la cuadrícula de manera más inteligente.

        :param num_plots: Número de gráficas
        :return: Tupla con filas y columnas
        """
        if num_plots > 12:
            raise ValueError("No se pueden generar más de 12 gráficas")

        # Mapeo predefinido para distribuciones óptimas
        grid_mapping = {
            1: (1, 1),  # 1 gráfica
            2: (1, 2),  # 2 gráficas
            3: (1, 3),  # 3 gráficas
            4: (2, 2),  # 4 gráficas
            5: (2, 3),  # 5 gráficas
            6: (2, 3),  # 6 gráficas
            7: (3, 3),  # 7 gráficas
            8: (3, 3),  # 8 gráficas
            9: (3, 3),  # 9 gráficas
            10: (3, 4),  # 10 gráficas
            11: (3, 4),  # 11 gráficas
            12: (3, 4)  # 12 gráficas
        }

        return grid_mapping.get(num_plots, (3, 4))

    def add_plot(self, plot_func):
        """
        Agrega una función de gráfica al canvas.

        :param plot_func: Función que genera la gráfica
        """
        if len(self.plots) >= 12:
            raise ValueError("Límite de 12 gráficas alcanzado")

        self.plots.append(plot_func)

    def show(self):
        """
        Muestra el canvas con todas las gráficas generadas.
        """
        num_plots = len(self.plots)
        rows, cols = self._calculate_grid(num_plots)

        # Cerrar figuras previas y aplicar tema de Seaborn
        plt.close('all')
        sns.set_theme()

        self.fig, self.axs = plt.subplots(rows, cols, figsize=(16, 10))

        if num_plots == 1:
            self.axs = np.array([self.axs])

        axs_flat = self.axs.ravel() if num_plots > 1 else [self.axs[0]]

        for i, plot_func in enumerate(self.plots):
            plot_func(self.data, axs_flat[i])

        for i in range(num_plots, len(axs_flat)):
            axs_flat[i].axis('off')

        plt.tight_layout()
        plt.show()

    def save(self, filename='dynamic_canvas.pdf'):
        """
        Guarda el canvas generado.

        :param filename: Nombre del archivo de salida
        """
        num_plots = len(self.plots)
        rows, cols = self._calculate_grid(num_plots)

        # Crear figura y subplots
        plt.close('all')  # Cerrar figuras previas
        self.fig, self.axs = plt.subplots(rows, cols, figsize=(16, 10))

        # Si solo hay un subplot, convertir a array
        if num_plots == 1:
            self.axs = np.array([self.axs])

        # Aplanar los axes para manejo más sencillo
        axs_flat = self.axs.ravel() if num_plots > 1 else [self.axs[0]]

        # Generar gráficas
        for i, plot_func in enumerate(self.plots):
            plot_func(self.data, axs_flat[i])

        # Ocultar subplots extras
        for i in range(num_plots, len(axs_flat)):
            axs_flat[i].axis('off')

        plt.tight_layout()
        os.makedirs('logs/eda_visuals/', exist_ok=True)
        plt.savefig('logs/eda_visuals/'+filename, format='pdf', dpi=500)
        plt.savefig('logs/eda_visuals/'+filename.replace('.pdf', '.png'), format='png', dpi=500)
