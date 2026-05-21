from pyspark.errors.exceptions.captured import AnalysisException
import logging


def load(df, path):
    try:
        #validacao basica : verifica se o df esta vazio
        if df.isEmpty():
            print("Aviso : Data Frame esta vazio.")
            return False

        print(f"Iniciando gravacao do dados em {path}")

        #tentativa de salvar
        df.write.mode("overwrite").parquet(path)
        return True
    except AnalysisException as e:
        logging.error(e)
        return False
    except Exception as e:
        logging.error(e)
        return False