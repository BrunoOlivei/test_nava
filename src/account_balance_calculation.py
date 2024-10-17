from typing import Union, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum, when, lag, lit, first, max
from pyspark.sql.window import Window

from src.logger import get_logger   

logger = get_logger()

spark = SparkSession.builder.appName("teste_nava").getOrCreate()

class AccountBalanceCalculation:
    def __init__(self, input_path: Union[List[str], str]):
        self.input_path = input_path

    def read_movimentation_data(self):
        df = None
        try:
            if isinstance(self.input_path, list):
                for path in self.input_path:
                    if "movimentacao" in path:
                        if df is None:
                            df = spark.read.csv(
                                path,
                                header=True,
                                sep=";"
                            )
                        else:   
                            df = df.union(
                                spark.read.csv(
                                    path,
                                    header=True,
                                    sep=";"
                                )
                            )
            else:
                df = spark.read.csv(
                    self.input_path,
                    header=True,
                    sep=";"
                )
            return df
        except Exception as e:
            logger.error(f"Error reading data: {e}")

    def read_initial_balance(self):
        try:
            for path in self.input_path:
                if "movimentacao" not in path:
                    return spark.read.csv(
                        path,
                        header=True,
                        sep=";"
                    )
        except Exception as e:
            logger.error(f"Error reading initial balance: {e}")
    
    def format_movimentations_data(self, df: DataFrame) -> DataFrame:
        try:
            return df.withColumn(
                "Saldo_Inicial",
                lit(0).cast("float")
            ).withColumn(
                "Credito",
                when(
                    col("Movimentacao_dia") > 0,
                    col("Movimentacao_dia").cast("float")
                ).otherwise(0)
            ).withColumn(
                "Debito",
                when(
                    col("Movimentacao_dia") < 0,
                    col("Movimentacao_dia").cast("float")
                ).otherwise(0)
            ).withColumn(
                "Movimentacao_dia",
                (col("Credito") + col("Debito")).cast("float")
            ).withColumn(
                "Saldo_Final",
                lit(0).cast("float")
            ).withColumnRenamed(
                "data",
                "Data"
            ).select(
                "Nome",
                "CPF",
                "Data",
                "Saldo_Inicial",
                "Credito",
                "Debito",
                "Movimentacao_dia",
                "Saldo_Final"
            )
        except Exception as e:
            logger.error(f"Error formatting movimentations data: {e}")

    def format_initial_balance(self, df: DataFrame) -> DataFrame:
        try:
            return df.withColumnRenamed(
                "Saldo_Inicial_CC",
                "Saldo_Inicial"
            ).withColumnRenamed(
                "data",
                "Data"
            ).withColumn(
                "Credito",
                lit(0).cast("float")
            ).withColumn(
                "Debito",
                lit(0).cast("float")
            ).withColumn(
                "Movimentacao_dia",
                (col("Credito") + col("Debito")).cast("float")
            ).withColumn(
                "Saldo_final",
                col("Saldo_Inicial").cast("float")
            ).select(
                "Nome",
                "CPF",
                "Data",
                "Saldo_Inicial",
                "Credito",
                "Debito",
                "Movimentacao_dia",
                "Saldo_final"
            )
        except Exception as e:
            logger.error(f"Error formatting initial balance: {e}")

    def filling_initial_balance(self, df: DataFrame) -> DataFrame:
        try:
            window_spec = Window.partitionBy("CPF").orderBy("Data")

            final_balance = df.withColumn(
                "Saldo_Inicial",
                lag("Saldo_Final", 1).over(window_spec)
            )

            final_balance = final_balance.withColumn(
                "Saldo_Inicial",
                when(
                    col("Saldo_Inicial").isNull(),
                    col("Saldo_Final")
                ).otherwise(
                    col("Saldo_Inicial")
                )
            )
            return final_balance
        except Exception as e:
            logger.error(f"Error filling final balance: {e}")

    def calculate_all_operations(self, df: DataFrame) -> DataFrame:
        try:
            window_spec = Window.partitionBy("CPF").orderBy("Data").rowsBetween(Window.unboundedPreceding, Window.currentRow)

            all_operations = df.withColumn(
                "Saldo_Final",
                sum(col("Saldo_Inicial") + col("Movimentacao_dia")).over(window_spec)
            )
        except Exception as e:
            logger.error(f"Error calculating balance: {e}")
        
        if all_operations.count() > 0:
            result = self.filling_initial_balance(all_operations)
            return result
        else:
            logger.error("No data to calculate balance")
            return None
        
    def save_data(self, df: DataFrame, file_name: str, path: str):
        try:
            df.write.csv(path + file_name, header=True, sep=";")
            logger.info("Data saved")
        except Exception as e:
            logger.error(f"Error saving data: {e}")

    def calculate_balance(self):
        logger.info("Reading data")
        movimentations = self.read_movimentation_data()
        initial_balance = self.read_initial_balance()
        logger.info("Formatting movimentations data")
        movimentations = self.format_movimentations_data(movimentations)
        logger.info("Formatting initial balance")
        initial_balance = self.format_initial_balance(initial_balance)
        df = initial_balance.union(movimentations)
        logger.info("Calculating balance")
        result = self.calculate_all_operations(df)
        return result
    
    def calculate_consolidated_balance(self, df: DataFrame) -> DataFrame:
        try:
            return df.groupBy('CPF').agg(
                first('Saldo_Inicial').alias('Saldo_Inicial'),
                sum('Movimentacao_dia').alias('Total_Movimentacao_dia'),
                max('Data').alias('Last_Transaction_Date')
            ).withColumn(
                'Real_Balance', col('Saldo_Inicial') + col('Total_Movimentacao_dia')
            ).show()
        except Exception as e:
            logger.error(f"Error calculating consolidated balance: {e}")
    
    def close(self):
        spark.stop()
        logger.info("Spark stopped")
