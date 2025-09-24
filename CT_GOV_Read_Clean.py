from __future__ import annotations
import re
import pandas as pd
from Utils import clean_selected_columns, to_datetime_cols, remove_punctuation_inplace, save_df


def CT_GOV_Cleaning(csv_path: str, output_path: str | None = None) -> pd.DataFrame:

    # 1) DATA CLEANING
    DATE_COLS = ["start_date", "primary_completion_date", "completion_date"]
    CSV_DO_CLEAN = True                  # set to False later to skip cleaning entirely
    CSV_STRIP_WS = True                  # Leading/Trailing whitespace
    CSV_COLLAPSE_WS = False              # tabs/line-breaks/dup spaces
    CSV_CASE_MODE = "upper"              # "none"|"upper"|"lower"|"title"
    CSV_REPLACE_NULLS_STRINGS = True
    CSV_REPLACE_NULLS_NUMBERS = True
    CSV_DROP_ROWS = True                 # drop rows all-empty across the cleaned scope (all cols here)
    CSV_DROP_COLS = True                 # drop columns that are entirely null after cleaning
    CSV_REMOVE_PUNCTUATION = True        # optional extra to mirror Alteryx "Punctuation"


    # 2) CREATING DATAFRAME
    
    CT_gov_initial = pd.read_csv(csv_path)
    CT_gov_initial = to_datetime_cols(CT_gov_initial, DATE_COLS)

    if CSV_DO_CLEAN:
        CT_gov_initial = clean_selected_columns(
        CT_gov_initial,
        fields_to_clean=None,
        replace_nulls_strings=CSV_REPLACE_NULLS_STRINGS,
        replace_nulls_numbers=CSV_REPLACE_NULLS_NUMBERS,
        strip_ws=CSV_STRIP_WS,
        collapse_ws=CSV_COLLAPSE_WS,
        case_mode=CSV_CASE_MODE,
        drop_rows=CSV_DROP_ROWS,
        drop_cols=CSV_DROP_COLS)
        if CSV_REMOVE_PUNCTUATION:
            remove_punctuation_inplace(CT_gov_initial)

    # 3) FILTERING DATA BASIS KEYWORDS
    terms = ['DRUG', 'BIOLOGICAL']
    keywords = '|'.join(map(re.escape, terms))
    CT_gov_initial['sex'].replace('ALL', 'BOTH', inplace=True)
    CT_gov_initial = CT_gov_initial.rename(columns={'NCT id': 'NCT ID','sex':'Patient Gender'}) 
    CT_gov_initial = CT_gov_initial[CT_gov_initial['interventions'].astype('string').str.contains(keywords, case=False, na=False)].copy()


    # 4) OPTIONAL OUTPUT (for quick inspection)
        # Save a DataFrame to CSV / Excel / Parquet based on the file extension
    if output_path:
        try:
            save_df(CT_gov_initial, output_path, index=False)
        except PermissionError as e:
            from pathlib import Path
            alt = Path(output_path).with_suffix(".csv")
            CT_gov_initial.to_csv(alt, index=False)
            print(f"[WARN] Couldn’t write Excel '{output_path}' "
                f"(likely open/locked): {e}\n"
                f"       Wrote CSV fallback: '{alt}'.")
        except Exception as e:
            print(f"[WARN] Skipping write of '{output_path}': {e}")
    return CT_gov_initial

