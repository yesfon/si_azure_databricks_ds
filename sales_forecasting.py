import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sktime.utils.plotting import plot_series
from utils import aggregate_country_sales_by_date
from sales_forecasting.sales_forecast_model import SalesForecastModel

warnings.filterwarnings("ignore")
country='United States'
data_generator = aggregate_country_sales_by_date(data_path='data/raw/SuperStore_Orders.csv', country=country)
forecaster = SalesForecastModel(date_col='order_date', target='sales')
forecaster.load_data(f'data/processed/{country}_sales_by_date.csv')
forecaster.preprocess()
y_train, y_test = forecaster.split_data()

gscv = forecaster.train_model()
metrics = forecaster.evaluate()

print(f"\nBest Model: {metrics['Best Model']}")
print(f"Test MAPE: {metrics['MAPE']:.2f}%")

# Predecir próximos 30 días
predictions = forecaster.predict(horizon=30)
print("\nForecast:")
print(predictions)

# Plot resultados
plot_series(y_train, y_test, predictions, labels=["Train", "Test", "Forecast"])
plt.show()