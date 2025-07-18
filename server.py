from mcp.server.fastmcp import FastMCP
import pandas as pd
import httpx
from httpx import HTTPError
from io import StringIO, BytesIO
from pathlib import Path
from functools import wraps

mcp = FastMCP("Dataset Tool")

# -------------------------------
# Classe per caricare dati
# -------------------------------
class DatasetLoader:
    def __init__(self, source: str):
        self.source = source

    def load(self) -> pd.DataFrame:
        if self._is_url():
            return self._load_from_url()
        elif self._is_local_file():
            return self._load_from_local()
        else:
            raise ValueError("Source non valido: fornire un URL http(s) o un path locale .csv/.xlsx")

    def _is_url(self) -> bool:
        return self.source.startswith("http://") or self.source.startswith("https://")

    def _is_local_file(self) -> bool:
        path = Path(self.source)
        return path.is_file() and path.suffix in [".csv", ".xlsx"]

    def _load_from_url(self) -> pd.DataFrame:
        try:
            response = httpx.get(self.source)
            response.raise_for_status()
            if self.source.endswith(".csv"):
                return pd.read_csv(StringIO(response.text))
            elif self.source.endswith(".xlsx"):
                return pd.read_excel(BytesIO(response.content))
            else:
                raise ValueError("Estensione file non supportata per URL: usare .csv o .xlsx")
        except HTTPError as e:
            raise ValueError(f"Errore durante il download del file: {e}")

    def _load_from_local(self) -> pd.DataFrame:
        path = Path(self.source)
        if not path.exists():
            raise FileNotFoundError(f"File locale non trovato: {path.resolve()}")
        if path.suffix == ".csv":
            return pd.read_csv(path, encoding='utf-8')
        elif path.suffix == ".xlsx":
            return pd.read_excel(path)
        else:
            raise ValueError("Estensione file non supportata: usare .csv o .xlsx")


# -------------------------------
# Decoratore per validazione dati
# -------------------------------
def requires_data_records(func):
    @wraps(func)
    def wrapper(data: list, *args, **kwargs):
        if not isinstance(data, list):
            raise ValueError("I dati devono essere una lista.")
        if not data:
            raise ValueError("I dati sono vuoti.")
        if not all(isinstance(row, dict) for row in data):
            raise ValueError("Ogni elemento deve essere un dizionario (record).")
        return func(data, *args, **kwargs)
    return wrapper


# -------------------------------
# Tool: Carica CSV/XLSX (URL o file)
# -------------------------------
@mcp.tool()
def load_dataset(source: str) -> list:
    """
    Carica un dataset CSV o XLSX da file locale o URL.
    """
    loader = DatasetLoader(source)
    df = loader.load()
    return df.to_dict(orient="records")


# -------------------------------
# Tool: Estrai colonne
# -------------------------------
@mcp.tool()
@requires_data_records
def get_columns(data: list) -> list:
    """
    Restituisce i nomi delle colonne del dataset.
    """
    df = pd.DataFrame(data)
    return df.columns.tolist()


# -------------------------------
# Tool: Query
# -------------------------------
@mcp.tool()
@requires_data_records
def query_dataframe(data: list, query: str) -> list:
    """
    Esegue una query Pandas su un dataset.
    """
    df = pd.DataFrame(data)
    try:
        result = df.query(query)
        return result.to_dict(orient="records")
    except Exception as e:
        raise ValueError(f"Errore nella query: {e}")
