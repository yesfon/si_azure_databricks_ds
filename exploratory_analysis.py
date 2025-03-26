import warnings
import pandas as pd
import seaborn as sns

from visuals.dynamic_canvas import DynamicCanvas
from exploratory_analysis.exploratory_data_analysis import ExploratoryDataAnalysis

warnings.filterwarnings('ignore')

data = pd.read_csv('data/raw/SuperStore_Orders.csv', encoding='latin-1')

eda = ExploratoryDataAnalysis(data, project_name='superstore')
canvas = DynamicCanvas(data)

canvas.add_plot(lambda df, ax: sns.countplot(x=df['segment'], ax=ax, palette = "Set2"))
canvas.add_plot(lambda df, ax: df['state'].value_counts().head(10).plot.pie(ax=ax))
canvas.add_plot(lambda df, ax: df['country'].value_counts().head(10).plot.bar(ax=ax,
                                                                              color = sns.color_palette("Paired")))
canvas.add_plot(lambda df, ax: (sns.countplot(x=df['region'], ax=ax, palette="Set2"),
                                ax.set_xticklabels(ax.get_xticklabels(), rotation=90, ha="right")))
canvas.show()
canvas.save('eda_visuals.pdf')

eda.run_full_eda()