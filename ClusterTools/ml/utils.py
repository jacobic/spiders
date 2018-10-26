from sklearn.base import TransformerMixin, BaseEstimator

class FeatureExtractor(TransformerMixin, BaseEstimator):
    def __init__(self, feature):
        self.factor = feature

    def transform(self, data):
        return data[self.factor]

    def fit(self, *_):
        return self


class ValuesExtractor(TransformerMixin, BaseEstimator):
    def transform(self, data):
        return data.values

    def fit(self, *_):
        return self


class DropColumns(TransformerMixin, BaseEstimator):
    def __init__(self, columns, axis):
        self.columns = columns
        self.axis = axis

    def transform(self, data):
        return data.drop(self.columns, axis=self.axis)

    def fit(self, *_):
        return self
