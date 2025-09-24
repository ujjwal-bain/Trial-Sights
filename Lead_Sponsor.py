from __future__ import annotations
import pandas as pd
from Utils import save_df

def add_lead_sponsor(Union_TT_CT: pd.DataFrame, output_path: str | None = None) -> pd.DataFrame:
    Union_TT_CT_Lead_Sponsor = Union_TT_CT.copy()

    # Prefer the TT-renamed column; fall back if needed
    src = Union_TT_CT_Lead_Sponsor.get("TT_Sponsor/Collaborator")
    if src is None:
        src = Union_TT_CT_Lead_Sponsor.get("Sponsor/Collaborator", pd.Series("", index=Union_TT_CT_Lead_Sponsor.index))

    Union_TT_CT_Lead_Sponsor["Bain_Lead Sponsor"] = (src.astype("string").fillna("")
           .str.split(r"[\r\n]+", regex=True).str[0]   # first line
           .str.split(",", n=1).str[0]                # before first comma
           .str.strip().str.title())

    if output_path:
        save_df(Union_TT_CT_Lead_Sponsor, output_path, index=False)

    return Union_TT_CT_Lead_Sponsor