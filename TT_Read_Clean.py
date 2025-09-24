from __future__ import annotations
import pandas as pd
from typing import Literal
from Utils import clean_selected_columns, to_datetime_cols, save_df


def TT_Cleaning(excel_path: str, sheet: str = "Results", output_path: str | None = None) -> pd.DataFrame:

    # 1) KEEP / DROP (Select tool)
    # Columns to keep from the Excel file (others are dropped)
    COLS_TO_KEEP: list[str] = [
        "Trial ID",  "Protocol/Trial ID", "Trial Title", "Trial Phase", "Trial Status",
        "Therapeutic Area","Disease", "MeSH Term", "Sponsor/Collaborator", 
        "Sponsor/Collaborator Type", "Sponsor/Collaborator: Parent HQ Country", 
        "Primary Tested Drug", "Primary Tested Drug: Mechanism Of Action",
        "Primary Tested Drug: Target", "Primary Tested Drug: Therapeutic Class", "Primary Tested Drug: Drug Type",
        "Other Tested Drug", "Other Tested Drug: Mechanism Of Action",
        "Other Tested Drug: Target", "Other Tested Drug: Therapeutic Class", "Other Tested Drug: Drug Type",
        "Oncology Biomarker", "Oncology Biomarker Common Use(s)",
        "Primary Endpoint", "Primary Endpoint Group",
        "Primary Endpoint Details", "Secondary/Other Endpoint",
        "Secondary/Other Endpoint Group", "Secondary/Other Endpoint Details",
        "Start Date", "Treatment Duration (Mos.)", 
        "Primary Completion Date", "Primary Completion Date Type", "Full Completion Date", "Primary Endpoints Reported Date",
        "Primary Endpoints Reported Date Type", "Pts/Site/Mo", "Patient Gender", "Patient Age Group", "Min Patient Age", "Min Patient Age Unit",
        "Max Patient Age", "Max Patient Age Unit", "Target Accrual", "Actual Accrual (No. of patients)", "Actual Accrual (% of Target)",
        "Reported Sites", "Identified Sites", "Trial Region", "Countries", "Countries Count", "ClinicalTrials.gov Location Country", 
        "ClinicalTrials.gov Sites Count", "Prior/Concurrent Therapy", "Treatment Plan", "Study Keywords", "Study Design",
        "Decentralized (DCT) Attributes", "Associated CRO", "Last Modified Date"]

    # 1.1) Renaming Cols
    RENAMES = {
        "Trial Status": "TT_Trial Status",
        "Protocol/Trial ID": "Protocol_Trial_ID",
        "Sponsor/Collaborator": "TT_Sponsor/Collaborator",
        "Primary Tested Drug: Target": "Primary Tested Drug: Target_truncated",
        "Other Tested Drug: Mechanism Of Action":"Other Tested Drug: Mechanism Of Action_truncated",
        "Other Tested Drug: Target": "Other Tested Drug: Target_truncated",
        "Other Tested Drug: Therapeutic Class": "Other Tested Drug: Therapeutic Class_truncated",
        "Primary Endpoint Group":"Primary Endpoint Group_truncated",
        "Primary Endpoint Details":"Primary Endpoint Details_truncated",
        "Secondary/Other Endpoint Group":"Secondary/Other Endpoint Group_truncated", 
        "Secondary/Other Endpoint Details":"Secondary/Other Endpoint Details_truncated",
        "Prior/Concurrent Therapy":"Prior/Concurrent Therapy_truncated",
        "Study Design":"TT_Study Design"}


    # 2) DATA CLEANING
    REMOVE_NULL_ROWS  = True   # drop rows where ALL selected columns are null
    REMOVE_NULL_COLS  = True   # drop columns that are entirely null

    # Choosing fields to be cleaned.
    # If empty, all present columns will be cleaned
    FIELDS_TO_CLEANSE: list[str] = [
        "Trial ID", "Protocol_Trial_ID", "Trial Title", "Trial Phase", 
        "Therapeutic Area","Disease", "Primary Tested Drug", "Primary Tested Drug: Mechanism Of Action",
        "Primary Tested Drug: Target_truncated", "Primary Tested Drug: Therapeutic Class",
        "Other Tested Drug", "Other Tested Drug: Mechanism Of Action_truncated",
        "Other Tested Drug: Target_truncated", "Other Tested Drug: Therapeutic Class_truncated",
        "Oncology Biomarker", "Oncology Biomarker Common Use(s)",
        "Primary Endpoint", "Primary Endpoint Group_truncated",
        "Primary Endpoint Details_truncated", "Secondary/Other Endpoint",
        "Secondary/Other Endpoint Group_truncated", "Secondary/Other Endpoint Details_truncated",
        "Start Date", "Primary Completion Date", "Primary Completion Date Type", "Primary Endpoints Reported Date",
        "Primary Endpoints Reported Date Type", "Patient Gender", "Patient Age Group", "Min Patient Age", "Min Patient Age Unit",
        "Max Patient Age", "Max Patient Age Unit", "Target Accrual", "Actual Accrual (No. of patients)", "Actual Accrual (% of Target)",
        "Reported Sites", "Identified Sites", "Trial Region", "Countries", "ClinicalTrials.gov Location Country", 
        "ClinicalTrials.gov Sites Count", "Prior/Concurrent Therapy_truncated", "Study Keywords", "Associated CRO", "Last Modified Date"]

    # Replace nulls
    REPLACE_NULLS_STRINGS = True
    REPLACE_NULLS_NUMBERS = True

    # Remove unwanted characters
    REMOVE_LEADING_TRAILING_WHITESPACE       = True
    REPLACE_TABS_LB_DUP_WHITESPACE_WITH_SPACE = True  # tabs, line breaks, duplicate whitespace will become a single space

    # Modify case: one of {"none","upper","lower","title"}
    MODIFY_CASE: Literal["none","upper","lower","title"] = "title"


    # 3) CREATING DATAFRAME WITH APPROPRIATE RENAMES AND TYPES
    TT_initial = pd.read_excel(excel_path, sheet_name=sheet)

    # Select only requested columns that actually exist
    keep = [c for c in COLS_TO_KEEP if c in TT_initial.columns]
    TT_initial = TT_initial[keep].copy()

    # Rename columns per your Select tool
    if RENAMES:
        TT_initial = TT_initial.rename(columns=RENAMES)

    # Keep date columns separate so as not include in cleaning
    TT_initial = to_datetime_cols(TT_initial, [
        "Start Date",
        "Primary Completion Date",
        "Full Completion Date",
        "Primary Endpoints Reported Date",
        "Last Modified Date"])
    
    # Change field type for "Trial ID" to Int64
    if "Trial ID" in TT_initial.columns:
        TT_initial["Trial ID"] = pd.to_numeric(TT_initial["Trial ID"], errors="coerce").astype("Int64")

    selected_cols = TT_initial.columns.tolist()

    TT_initial = clean_selected_columns(
        TT_initial,
        fields_to_clean=FIELDS_TO_CLEANSE,
        replace_nulls_strings=REPLACE_NULLS_STRINGS,
        replace_nulls_numbers=REPLACE_NULLS_NUMBERS,
        strip_ws=REMOVE_LEADING_TRAILING_WHITESPACE,
        collapse_ws=REPLACE_TABS_LB_DUP_WHITESPACE_WITH_SPACE,
        case_mode=MODIFY_CASE,
        drop_rows=REMOVE_NULL_ROWS,
        drop_cols=REMOVE_NULL_COLS)

    final_cols = [c for c in selected_cols if c in TT_initial.columns]
    TT_initial = TT_initial[final_cols]

    base_cols = TT_initial.columns.tolist()
    TT_initial["NCT_Code_Position"] = (TT_initial["Protocol_Trial_ID"].astype("string").str.upper().str.find("NCT")).astype("Int64")
    TT_initial["NCT ID"] = (TT_initial["Protocol_Trial_ID"].astype("string").str.extract(r"(?i)(NCT\d{8})", expand=False).where(TT_initial["NCT_Code_Position"].fillna(-1).ge(0)).fillna("No NCT Code").astype("string"))
    temp_column = TT_initial["Patient Age Group"].astype("string")
    TT_initial["child"] = temp_column.str.contains(r"\bChildren\b",case=False, na=False)
    TT_initial["adult"] = temp_column.str.contains(r"\bAdults\b",case=False, na=False)
    TT_initial["older_adults"] = temp_column.str.contains(r"\bOlder Adults\b", case=False, na=False)


    # 5) CLEANING NEW CREATED COLUMN
    TT_initial = clean_selected_columns(
        TT_initial,
        fields_to_clean=["NCT ID"],
        replace_nulls_strings=True,
        replace_nulls_numbers=True,
        strip_ws=True,
        collapse_ws=False,
        case_mode="upper",
        drop_rows=False,
        drop_cols=False)
    
    final_cols = ([c for c in base_cols if c in TT_initial.columns]+ ["NCT ID", "child", "adult", "older_adults"])
    TT_initial = TT_initial[[c for c in final_cols if c in TT_initial.columns]]

    # 6) OPTIONAL OUTPUT (for quick inspection)
    # Save a DataFrame to CSV / Excel / Parquet based on the file extension
    
    if output_path:                         # only try if a path was provided
        try:
            save_df(TT_initial, output_path, index=False)
        except PermissionError as e:
            # common on Windows when Excel/OneDrive keeps the file open
            from pathlib import Path
            alt = Path(output_path).with_suffix(".csv")
            TT_initial.to_csv(alt, index=False)
            print(f"[WARN] Couldn’t write Excel '{output_path}' "
                f"(likely open/locked): {e}\n"
                f"       Wrote CSV fallback: '{alt}'.")
        except Exception as e:
            print(f"[WARN] Skipping write of '{output_path}': {e}")
    return TT_initial
    
    #----------------------------------------------------------------------------------------------------------------------