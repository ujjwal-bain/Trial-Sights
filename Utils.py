from __future__ import annotations
from pathlib import Path
from typing import Literal, Iterable
import pandas as pd
from pandas.api.types import (is_numeric_dtype, is_string_dtype, is_object_dtype,
    is_datetime64_any_dtype, is_bool_dtype)

# Text cleaning (Series)
def clean_text_series(s: pd.Series, strip_ws: bool, collapse_ws: bool,case_mode: str) -> pd.Series:
    s = s.astype("string")
    if strip_ws:
        s = s.str.strip()
    if collapse_ws:
        s = s.str.replace(r"[\t\r\n]+", " ", regex=True)  # tabs/linebreaks -> space
        s = s.str.replace(r"\s{2,}", " ", regex=True)    # collapse multiple spaces
    if case_mode == "upper":
        s = s.str.upper()
    elif case_mode == "lower":
        s = s.str.lower()
    elif case_mode == "title":
        s = s.str.title()
    return s


# DataFrame cleaning (select columns, fill nulls, text rules, drop all-empty rows/cols)
def clean_selected_columns(
    df: pd.DataFrame,
    fields_to_clean: list[str] | None,
    replace_nulls_strings: bool,
    replace_nulls_numbers: bool,
    strip_ws: bool,
    collapse_ws: bool,
    case_mode: Literal["none","upper","lower","title"],
    drop_rows: bool | None = None,
    drop_cols: bool | None = None) -> pd.DataFrame:
    out = df.copy()

    drop_rows = True if drop_rows is None else drop_rows
    drop_cols = True if drop_cols is None else drop_cols

    # only touch listed columns that exist
    present = [c for c in (fields_to_clean or list(out.columns)) if c in out.columns]

    # partition by dtype
    num_cols  = [c for c in present if is_numeric_dtype(out[c])]
    dt_cols   = [c for c in present if is_datetime64_any_dtype(out[c])]
    txt_cols  = [c for c in present if (is_string_dtype(out[c]) or is_object_dtype(out[c])) and not is_datetime64_any_dtype(out[c])]

    # null fills
    if replace_nulls_numbers and num_cols:
        out[num_cols] = out[num_cols].fillna(0)

    if replace_nulls_strings and txt_cols:
        for c in txt_cols:
            out[c] = out[c].astype("string").fillna("")

    # text cleanup (strings only, dates remain as is)
    for c in txt_cols:
        out[c] = clean_text_series(out[c], strip_ws, collapse_ws, case_mode)

    # drop rows where all cols are empty/null
    if present and drop_rows:
        def is_empty(col: pd.Series) -> pd.Series:
            if is_numeric_dtype(col) or is_bool_dtype(col) or is_datetime64_any_dtype(col):
                return col.isna()                         # 0 is NOT empty
            return col.astype("string").str.len().fillna(0).eq(0)
        null_like = out[present].apply(is_empty)
        out = out[~null_like.all(axis=1)]

    # drop fully-null columns
    if drop_cols and present:
        cols_to_drop = [c for c in present if out[c].isna().all()]
        out = out.drop(columns=cols_to_drop)

    return out

# Date conversion function
def to_datetime_cols(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce")
    return out

# Punctuation removal for CT Gov
def remove_punctuation_inplace(df: pd.DataFrame, columns: Iterable[str] | None = None) -> None:
    cols = list(columns) if columns else [
        c for c in df.columns
        if not is_numeric_dtype(df[c]) and not is_datetime64_any_dtype(df[c])
    ]
    for c in cols:
        df[c] = df[c].astype("string").str.replace(r"[^\w\s]", "", regex=True)

# Save DataFrame by extension
def save_df(df: pd.DataFrame, path: str | Path, index: bool = False) -> None:
    outp = Path(path)
    outp.parent.mkdir(parents=True, exist_ok=True)
    ext = outp.suffix.lower()
    writers = {
        ".csv": df.to_csv,
        ".xlsx": df.to_excel,
        ".xls": df.to_excel,
        ".parquet": df.to_parquet,
    }
    if ext not in writers:
        raise ValueError(f"Unsupported extension: {ext} (use .csv, .xlsx, .xls, .parquet)")
    writers[ext](outp, index=index)
