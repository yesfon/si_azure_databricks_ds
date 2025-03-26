import numpy as np
import pandas as pd
from scipy import stats
from sktime.utils import plot_windows
from sktime.forecasting.arima import ARIMA
from sktime.split import temporal_train_test_split
from sktime.forecasting.theta import ThetaForecaster
from sktime.forecasting.naive import NaiveForecaster
from sktime.forecasting.compose import make_reduction
from sktime.forecasting.base import ForecastingHorizon
from sktime.transformations.series.impute import Imputer
from sktime.forecasting.exp_smoothing import ExponentialSmoothing
from sktime.forecasting.compose import TransformedTargetForecaster
from sktime.performance_metrics.forecasting import MeanAbsolutePercentageError
from sktime.forecasting.model_selection import ExpandingWindowSplitter, ForecastingGridSearchCV

from xgboost import XGBRegressor
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor


class SalesForecastModel:
    def __init__(self, date_col='date', target='sales', test_size=0.2):
        self.date_col = date_col
        self.target = target
        self.test_size = test_size
        self.best_model = None
        self.metrics = {}
        self.freq = None

    def load_data(self, data_path):
        self.df = pd.read_csv(data_path, parse_dates=[self.date_col])
        self.df = self.df.sort_values(self.date_col).set_index(self.date_col)
        self.freq = pd.infer_freq(self.df.index)
        self.y = self.df[[self.target]]
        return self.df

    def preprocess(self):
        window, iqr_factor = 60, 3.5
        self.y = (
            self.y[~self.y.index.duplicated(keep='first')]
            .reindex(pd.date_range(start=self.y.index.min(), end=self.y.index.max(), freq='D'))
            .interpolate(method='time', limit_direction='both', limit_area='inside')
            .pipe(lambda df: df.where(
                (df >= df.rolling(window).median() - iqr_factor * df.rolling(window).apply(stats.iqr)) &
                (df <= df.rolling(window).median() + iqr_factor * df.rolling(window).apply(stats.iqr)),
                other=np.nan
            ))
            .interpolate(method='time')
            .ffill()
            .bfill()
            .asfreq('D')
        )
        self.freq = 'D'
        return self.y

    def split_data(self):
        self.y_train, self.y_test = temporal_train_test_split(self.y, test_size=self.test_size)
        return self.y_train, self.y_test

    def _create_pipeline(self):
        return TransformedTargetForecaster(steps=[
            ("imputer", Imputer()),
            ("forecaster", NaiveForecaster())
        ])

    def _get_param_grid(self):
        return [
            {
                "forecaster": [NaiveForecaster(sp=12)],
                "forecaster__strategy": ["drift", "last", "mean"],
            },
            {
                "forecaster": [ThetaForecaster(sp=12)],
                "imputer__method": ["mean", "drift"],
            },
            {
                "forecaster": [ExponentialSmoothing(sp=12)],
                "forecaster__trend": ["add", "mul"],
                "imputer__method": ["mean", "median"],
            },
            {
                "forecaster": [ARIMA(order=(1, 1, 1))],
                "forecaster__order": [(1, 1, 1), (2, 1, 2), (3, 1, 3)],
            },
            {
                "forecaster": [make_reduction(
                    LinearRegression(),
                    window_length=24,
                    strategy="recursive")],
                "forecaster__window_length": [12, 24],
            },
            {
                "forecaster": [make_reduction(
                    RandomForestRegressor(n_estimators=100),
                    window_length=24,
                    strategy="recursive")],
                "forecaster__window_length": [12, 24],
                "forecaster__estimator__n_estimators": [50, 100],
            },
            {
                "forecaster": [make_reduction(
                    XGBRegressor(),
                    window_length=24,
                    strategy="recursive")],
                "forecaster__window_length": [12, 24],
                "forecaster__estimator__n_estimators": [50, 100],
                "forecaster__estimator__max_depth": [3, 5],
                "forecaster__estimator__learning_rate": [0.1, 0.05],
            }
        ]

    def train_model(self):
        cv = ExpandingWindowSplitter(
            initial_window=len(self.y_train) // 2,
            step_length=30,
            fh=[x for x in range(1, 181)]
        )

        pipe = self._create_pipeline()

        gscv = ForecastingGridSearchCV(
            forecaster=pipe,
            param_grid=self._get_param_grid(),
            cv=cv,
            scoring=MeanAbsolutePercentageError(),
            n_jobs=-1
        )
        _ = plot_windows(cv=cv, y=self.y_train)
        gscv.fit(self.y_train)
        self.best_model = gscv.best_forecaster_
        return gscv

    def evaluate(self):
        fh = ForecastingHorizon(self.y_test.index, is_relative=False)
        y_pred = self.best_model.predict(fh)

        mape = MeanAbsolutePercentageError()(self.y_test, y_pred)
        self.metrics = {
            'MAPE': mape,
            'Best Model': type(self.best_model.steps[-1][1]).__name__
        }
        return self.metrics

    def predict(self, horizon=7):
        last_date = self.y.index[-1]
        fh = ForecastingHorizon(
            pd.date_range(start=last_date, periods=horizon + 1, freq=self.freq)[1:],
            is_relative=False
        )
        return self.best_model.predict(fh)
