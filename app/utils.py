import pandas as pd


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame column names to lowercase with underscores instead of spaces."""
    columns = {col: col.lower().replace(' ', '_') for col in df.columns}
    return df.rename(columns=columns)


def get_google_sheet_data(file_id: str, gid: int) -> pd.DataFrame:
    """ Fetch data from a Google Sheet as a CSV and return it as a cleaned DataFrame."""
    url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv&gid={gid}"
    return (
        pd.read_csv(url)
        .pipe(clean_column_names)
    )