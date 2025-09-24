# Revenue_Mapping.py
from __future__ import annotations
import pandas as pd
from pathlib import Path
from Utils import save_df, clean_selected_columns, clean_text_series

def _read_mapping(path: str, sheet=0) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Mapping file not found: {p}")

    print(f"[Revenue_Mapping] Reading mapping file: {p} | sheet={sheet} | ext={p.suffix.lower()}")

    ext = p.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(p)

    # Try openpyxl first for modern Excel; then fall back gracefully
    try:
        import openpyxl  # noqa: F401
        return pd.read_excel(p, sheet_name=sheet, engine="openpyxl")
    except Exception as e_openpyxl:
        # Fallback 1: let pandas auto-pick an engine
        try:
            return pd.read_excel(p, sheet_name=sheet)   # engine=None
        except Exception as e_auto:
            # Fallback 2: xlrd (only works for legacy .xls and xlrd<=1.2)
            try:
                import xlrd  # noqa: F401
                return pd.read_excel(p, sheet_name=sheet, engine="xlrd")
            except Exception as e_xlrd:
                # Give a very explicit error so you know what's wrong with the file
                raise RuntimeError(
                    f"Failed to read mapping '{p}'.\n"
                    f" - openpyxl error: {type(e_openpyxl).__name__}: {e_openpyxl}\n"
                    f" - autodetect error: {type(e_auto).__name__}: {e_auto}\n"
                    f" - xlrd error: {type(e_xlrd).__name__}: {e_xlrd}\n"
                    f"Check that the file is a real Excel workbook (.xlsx/.xls), not a CSV or renamed file, "
                    f"and that the sheet name/index exists."
                ) from e_xlrd

def _norm_key(s: pd.Series, *, remove_punct: bool = True) -> pd.Series:
    
    s = clean_text_series(s.astype("string"), strip_ws=True, collapse_ws=True, case_mode="lower")
    if remove_punct:
        s = s.str.replace(r"[^\w\s]", "", regex=True)
    return s

def _clean_for_mapping(df: pd.DataFrame, fields_to_clean: list[str] | None,*, title_case: bool = True, remove_punct: bool = True) -> pd.DataFrame:
    out = clean_selected_columns(df=df, fields_to_clean=fields_to_clean, replace_nulls_strings=True, replace_nulls_numbers=True,
        strip_ws=True, collapse_ws=True, case_mode=("title" if title_case else "none"), drop_rows=False, drop_cols=False)
    if remove_punct:
        cols = fields_to_clean or [c for c in out.columns if out[c].dtype == "object" or str(out[c].dtype) == "string"]
        for c in cols:
            out[c] = out[c].astype("string").str.replace(r"[^\w\s]", "", regex=True)
    return out

def _find_replace_append(left: pd.DataFrame, lookup: pd.DataFrame, *, left_key: str, right_key: str | None = None, add_cols: list[str] | None = None) -> pd.DataFrame:
    if right_key is None:
        right_key = left_key
    L = left.copy()
    R = lookup.copy()

    L["_k"] = _norm_key(L[left_key]) if left_key in L.columns else ""
    R["_k"] = _norm_key(R[right_key]) if right_key in R.columns else ""

    if add_cols is None:
        add_cols = [c for c in R.columns if c not in (right_key, "_k")]

    out = L.merge(R[["_k", *add_cols]], on="_k", how="left").drop(columns=["_k"])
    return out

def map_revenue(union_with_lead: pd.DataFrame, *, 
    # mapping 1: Bain_Lead Sponsor -> EP Standard Name
    map1_path: str, map1_sheet: str | int = 0, map1_left_col: str = "Bain_Lead Sponsor", map1_right_key: str = "Bain_Lead Sponsor", map1_add_cols: list[str] | None = ["EP Standard Name"],

    # mapping 2: EP 2023 (US segmentation)
    map_us_path: str, map_us_sheet: str | int = 0, map_us_key: str = "EP Standard Name", map_us_add_cols: list[str] | None = ["US company segmentation"],

    # mapping 3: EP 2024 (WW segmentation)
    map_ww_path: str, map_ww_sheet: str | int = 0, map_ww_key: str = "EP Standard Name", map_ww_add_cols: list[str] | None = ["WW company segmentation"],

    # cleaning toggles (set remove_punct/title_case to mirror Alteryx checkboxes)
    cleanse_inputs: bool = True, remove_punct: bool = True, title_case: bool = True,

    # optional save
    output_path: str | None = None,) -> pd.DataFrame:
    """
    union_with_lead
      -> F&R with mapping 1 (append EP Standard Name)
      -> F&R with EP 2023 US (append US segmentation; fillna='Others')
      -> F&R with EP 2024 WW (append WW segmentation; fillna='Others')
    """
    # 1) read mapping tables
    map1  = _read_mapping(map1_path,  map1_sheet)
    map_us = _read_mapping(map_us_path, map_us_sheet)
    map_ww = _read_mapping(map_ww_path, map_ww_sheet)

    # 2) clean mapping inputs using your utilities (no row/col drops)
    if cleanse_inputs:
        # You can restrict columns; here we keep it simple and clean all string cols (pass None)
        map1  = _clean_for_mapping(map1,  fields_to_clean=None, title_case=title_case, remove_punct=remove_punct)
        map_us = _clean_for_mapping(map_us, fields_to_clean=None, title_case=title_case, remove_punct=remove_punct)
        map_ww = _clean_for_mapping(map_ww, fields_to_clean=None, title_case=title_case, remove_punct=remove_punct)

    # 3) Find & Replace #1: append EP Standard Name using Bain_Lead Sponsor
    s1 = _find_replace_append(union_with_lead, map1, left_key=map1_left_col, right_key=map1_right_key, add_cols=map1_add_cols)

    # 4) Find & Replace #2: append US segmentation on EP Standard Name
    s2 = _find_replace_append(s1, map_us, left_key=map_us_key, right_key=map_us_key, add_cols=map_us_add_cols)
    if map_us_add_cols:
        col = map_us_add_cols[0]
        mask = s2[col].astype("string").fillna("").eq("")
        t = s2.loc[mask].copy(); t[col] = "Others"       
        f = s2.loc[~mask]                                
        s2 = pd.concat([f, t]).sort_index(kind="stable")

    # 5) Find & Replace #3: append WW segmentation on EP Standard Name
    s3 = _find_replace_append(s2, map_ww, left_key=map_ww_key, right_key=map_ww_key, add_cols=map_ww_add_cols)
    if map_ww_add_cols:
        col = map_ww_add_cols[0]
        mask = s3[col].astype("string").fillna("").eq("")
        t = s3.loc[mask].copy(); t[col] = "Others"
        f = s3.loc[~mask]
        s3 = pd.concat([f, t]).sort_index(kind="stable")

    if output_path:
        save_df(s3, output_path, index=False)

    return s3
