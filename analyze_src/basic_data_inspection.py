import pandas as pd

from abc import abstractmethod, ABC


class DataInspectionStrategy(ABC):
    @abstractmethod
    def inspect(self, df: pd.DataFrame):
        pass
    
class DataTypeInspectionStrategy(DataInspectionStrategy):
    def inspect(self, df: pd.DataFrame):
            print("\nDatatype and Non-null Counts")
            print(df.info())

class SummaryStatisticsInspectionStrategy(DataInspectionStrategy):
    def inspect(self, df: pd.DataFrame):
        print("\nSummary statistics (Numerical Features)")       
        print(df.describe())     
        print("\nSummary statistics (Categorical Features)")
        print(df.describe(include=["O"]))

    
class DataInspector():
    def __init__(self, strategy:DataInspectionStrategy):
        self._strategy=strategy
    
    def set_strategy(self, strategy: DataInspectionStrategy):
        self._strategy = strategy
        
    def execute_inspection(self, df: pd.DataFrame):
        self._strategy.inspect(df)    



if __name__ == "__main__":
    df = pd.read_csv("../Data/raw/bank-additional/bank-additional-full.csv", delimiter=";")
    
    inspector = DataInspector(DataTypeInspectionStrategy())
    inspector.execute_inspection(df)
    
    inspector.set_strategy(SummaryStatisticsInspectionStrategy())
    inspector.execute_inspection(df)