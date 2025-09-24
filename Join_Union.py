from __future__ import annotations
from typing import Iterable, Tuple
import pandas as pd
import numpy as np
from pathlib import Path
import re
from pandas.api.types import is_object_dtype
from Utils import save_df

def _prep_right_subset(ct_df: pd.DataFrame, right_cols: Iterable[str]) -> pd.DataFrame:
    keep = set(right_cols) if right_cols is not None else set()
    keep.add("NCT ID")  # join key must be present
    keep = [c for c in keep if c in ct_df.columns] # Intersect with actual columns to be safe
    return ct_df.loc[:, keep].copy()

def _sanitize_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    #Remove illegal control chars and truncate long cells to Excel's limit
    out = df.copy()
    bad = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")
    for c in out.columns:
        if is_object_dtype(out[c]) or str(out[c].dtype) == "string":
            s = out[c].astype("string").fillna("")
            s = s.str.replace(bad, "", regex=True)
            s = s.str.slice(0, 32760)  # Excel cell text limit is 32767
            out[c] = s
    return out

def join_tt_ct_on_nct(tt_df: pd.DataFrame, ct_df: pd.DataFrame, right_cols_to_keep: Iterable[str], suffix_for_ct: str = "_CT") -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    # 1) Reduce CT to selected columns (+ key)
    ct_sub = _prep_right_subset(ct_df, right_cols_to_keep)

    # 2) Build a mapping of CT columns to rename with a suffix
    ct_nonkey = [c for c in ct_sub.columns if c != "NCT ID"]
    ct_sub_renamed = ct_sub.rename(columns={c: f"{c}{suffix_for_ct}" for c in ct_nonkey})

    # 3) Do a full indicator merge to partition into L/J/R like Alteryx
    merged = tt_df.merge(
        ct_sub_renamed,
        how="outer",               # full outer so we can extract all three buckets
        left_on="NCT ID",
        right_on="NCT ID",
        indicator=True,
        suffixes=("", suffix_for_ct))
    

    # 4) Partition results
    join_df = merged[merged["_merge"] == "both"].drop(columns=["_merge"])
    left_only_df = merged.loc[merged["_merge"] == "left_only", tt_df.columns].copy()
    right_only_df = merged[merged["_merge"] == "right_only"].drop(columns=[c for c in tt_df.columns if c in merged.columns] + ["_merge"])

    return join_df, left_only_df, right_only_df

def _tt_text_blob(df: pd.DataFrame) -> pd.Series:
    def S(col: str) -> pd.Series:
        return df[col].astype("string") if col in df.columns else pd.Series("", index=df.index, dtype="string")
    return S("TT_Study Design").str.cat([S("Treatment Plan"), S("Study Keywords")], sep=" ", na_rep="").str.upper()


PATT_STAGE1_BASE = r"(RANDOM|CONTROL|DOUBLE[\s-]?BLIND|PLACEBO|INTERVENTION)"

def stage1_base_filter(df: pd.DataFrame) -> pd.DataFrame:
    blob = _tt_text_blob(df)
    mask = blob.str.contains(PATT_STAGE1_BASE, regex=True, na=False)
    return df.loc[mask].copy()

# Stage-2: refine by checkboxes on the already base-filtered rows
PATT_INTERVENTIONAL = PATT_STAGE1_BASE  # same six tokens
PATT_OBSERVATIONAL  = r"(OBSERVATION|NON[\s-]?INTERVENTIONAL)"  # covers hyphen/space

def add_study_type_column(df: pd.DataFrame) -> pd.DataFrame:
    """Create Bain_StudyType = Interventional / Observational / Ambiguous / Unknown
       by scanning TT_Study Design, Treatment Plan, Study Keywords."""
    blob = _tt_text_blob(df)

    inter_mask = blob.str.contains(PATT_INTERVENTIONAL, regex=True, na=False)
    obs_mask   = blob.str.contains(PATT_OBSERVATIONAL,  regex=True, na=False)

    df = df.copy()
    df["Bain_StudyType"] = np.select(
        [
            inter_mask & ~obs_mask,
            obs_mask & ~inter_mask,
            inter_mask &  obs_mask,
        ],
        ["Interventional", "Observational", "Ambiguous"],
        default="Unknown"
    ).astype("string")
    return df

def stage2_refine_by_flags(df: pd.DataFrame, interventional: bool | None, observational: bool | None) -> pd.DataFrame:
    # If neither box is checked/provided -> skip refinement
    if not interventional and not observational:
        return df.copy()

    # Ensure the intermediary column exists
    if "Bain_StudyType" not in df.columns:
        df = add_study_type_column(df)

    selected = []
    if interventional: selected.append("Interventional")
    if observational:  selected.append("Observational")
    # If both boxes are checked, include ambiguous “both-signals” rows too
    if interventional and observational:
        selected.append("Ambiguous")

    return df.loc[df["Bain_StudyType"].isin(selected)].copy()

def run_join_operation(
    TT_Initial: pd.DataFrame,
    CT_GOV_Initial: pd.DataFrame,
    right_cols_to_keep: Iterable[str] | None = None,
    suffix_for_ct: str = "_CT",
    output_left_path: str | None = None,
    output_join_path: str | None = None,
    output_union_path: str | None = None,
    debug: bool = False, study_interventional: bool | None = None, study_observational: bool | None = None,
    sponsor_industry:  bool | None = None,
    sponsor_academic:  bool | None = None,
    sponsor_others:    bool | None = None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    
    if right_cols_to_keep is None:
        right_cols_to_keep = ["NCT ID", "study title", "study status", "interventions", "condition"]    # CT columns to be used for JOIN (J)
    join_df, left_only_df, right_only_df = join_tt_ct_on_nct(tt_df=TT_Initial, ct_df=CT_GOV_Initial, right_cols_to_keep=right_cols_to_keep, suffix_for_ct="_CT")

    Left_only_TT_CT = left_only_df
    Join_TT_CT = join_df

    # Optional
      
    if output_join_path:
        try:
            safe_join = _sanitize_for_excel(Join_TT_CT)
            save_df(safe_join, output_join_path, index=False)
        except Exception as e:
            # Fallback to CSV so your run still completes
            alt = Path(output_join_path).with_suffix(".csv")
            safe_join.to_csv(alt, index=False)
            print(f"[WARN] Couldn’t write Excel '{output_join_path}': {e}\n"
                f"        Wrote CSV fallback: '{alt}'.")

    Left_only_TT_CT = Left_only_TT_CT.loc[Left_only_TT_CT["NCT ID"].astype("string").str.strip().str.casefold().eq("no nct code").fillna(False)].copy()
    Left_only_TT_CT["Bain_Cleaned Sponsor/Collaborator Type"] = (Left_only_TT_CT["Sponsor/Collaborator Type"].astype("string")
        .str.split(r"[\r\n]+", regex=True).str[0]   # first line
        .str.split(",", n=1).str[0]                # before first comma
        .str.strip().str.title())
    tag = Left_only_TT_CT["Bain_Cleaned Sponsor/Collaborator Type"].astype("string").str.strip()
    mc = tag.str.casefold()
    Left_only_TT_CT["Bain_Cleaned Sponsor/Collaborator Type_tagged"] = np.select([mc.eq("industry"), mc.eq("academic")],["Industry", "Academic"],default="Others")
    selected_tags = []
    if sponsor_industry:  selected_tags.append("Industry")
    if sponsor_academic:  selected_tags.append("Academic")
    if sponsor_others:    selected_tags.append("Others")

    # Apply only if at least one box is checked; otherwise skip this refinement
    if selected_tags:
        Left_only_TT_CT = Left_only_TT_CT.loc[
            Left_only_TT_CT["Bain_Cleaned Sponsor/Collaborator Type_tagged"].isin(selected_tags)
        ].copy()

    Left_only_TT_CT = stage1_base_filter(Left_only_TT_CT)
    Left_only_TT_CT = stage2_refine_by_flags(Left_only_TT_CT, interventional=study_interventional, observational=study_observational)


    Union_TT_CT = pd.concat([Left_only_TT_CT, Join_TT_CT], axis=0, ignore_index=True, sort=False)

    if output_union_path:
        try:
            safe_join = _sanitize_for_excel(Union_TT_CT)
            save_df(safe_join, output_union_path, index=False)
        except Exception as e:
            # Fallback to CSV so your run still completes
            alt = Path(output_union_path).with_suffix(".csv")
            safe_join.to_csv(alt, index=False)
            print(f"[WARN] Couldn’t write Excel '{output_union_path}': {e}\n"
                f"        Wrote CSV fallback: '{alt}'.")

    if output_left_path:
        try:
            safe_join = _sanitize_for_excel(Left_only_TT_CT)
            save_df(safe_join, output_left_path, index=False)
        except Exception as e:
            # Fallback to CSV so your run still completes
            alt = Path(output_left_path).with_suffix(".csv")
            safe_join.to_csv(alt, index=False)
            print(f"[WARN] Couldn’t write Excel '{output_left_path}': {e}\n"
                f"        Wrote CSV fallback: '{alt}'.")
    
    
    return Left_only_TT_CT, Join_TT_CT, Union_TT_CT