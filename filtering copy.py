import pandas as pd

def apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    """
    Transforms and filters the trials DataFrame based on user-provided filters.
    - Start window is inclusive; end window is exclusive.
    - Respects multi-selects for phases/designs/statuses/TA/sponsor types/sponsors.
    - Region filter is applied only if `region_enabled` is True; 'Global' means no region filter.
    """

    # ----- helpers -----
    def colexists(name): 
        return name in df.columns

    def normalize_list(v):
        """Return a list or empty list. Treat None/'' as empty."""
        if v is None:
            return []
        if isinstance(v, (list, tuple, set)):
            return list(v)
        return [v]

    def is_effective_list(lst, all_tokens=("ALL", "ALL SPONSORS INCLUDED", "ANY")):
        """Return True if lst should be used for filtering (i.e., not empty and not an 'all' sentinel)."""
        up = [str(x).strip().upper() for x in lst]
        return len(up) > 0 and not any(tok in up for tok in all_tokens)

    # Pull window bounds
    start_month = int(filters.get("start_date_from_month", 1))
    start_year  = int(filters.get("start_date_from_year", 2015))
    end_month   = int(filters.get("start_date_until_month", 1))
    end_year    = int(filters.get("start_date_until_year", 2015))

    region_enabled = bool(filters.get("region_enabled", False))
    region_value   = str(filters.get("region", "Global"))

    selected_phases              = normalize_list(filters.get("selected_phases"))
    selected_study_designs       = normalize_list(filters.get("selected_study_designs"))
    selected_trial_statuses      = normalize_list(filters.get("selected_trial_statuses"))
    selected_lead_sponsor_types  = normalize_list(filters.get("selected_lead_sponsor_types"))
    selected_therapeutic_areas   = normalize_list(filters.get("selected_therapeutic_areas"))
    selected_sponsors            = normalize_list(filters.get("selected_sponsors"))

    data_source = str(filters.get("data_source", "")).strip()

    df = df.copy()

    # ---- base cleanup / transforms ----
    # filter out Planned
    if colexists("TT_Trial Status"):
        df = df[df["TT_Trial Status"].fillna("").ne("Planned")]

    # coerce dates
    if colexists("Start Date"):
        df["Start Date"] = pd.to_datetime(df["Start Date"], errors="coerce", utc=True)
        # derive start year/month columns
        df["Bain_Start Year"] = df["Start Date"].dt.year.astype("Int64")
        df["Bain_Start Month"] = df["Start Date"].dt.month.astype("Int64")
    else:
        # If Start Date missing, create nullable cols to avoid downstream errors
        df["Start Date"] = pd.NaT
        df["Bain_Start Year"] = pd.Series(pd.array([pd.NA] * len(df), dtype="Int64"))
        df["Bain_Start Month"] = pd.Series(pd.array([pd.NA] * len(df), dtype="Int64"))

    if colexists("Last Modified Date"):
        df["Last Modified Date"] = pd.to_datetime(df["Last Modified Date"], errors="coerce", utc=True)
    else:
        df["Last Modified Date"] = pd.NaT

    # month-aware window: [start, end)
    start_bound = pd.Timestamp(start_year, start_month, 1, tz="UTC")
    end_bound   = pd.Timestamp(end_year,   end_month,   1, tz="UTC")
    if colexists("Start Date"):
        df = df[(df["Start Date"] >= start_bound) & (df["Start Date"] < end_bound)]

    # Bain_Therapeutic Area (whitelist else 'Multiple')
    whitelist = {
        "Oncology", "Cns", "Unassigned", "Metabolic/Endocrinology", "Autoimmune/Inflammation",
        "Infectious Disease", "Cardiovascular", "Infectious Disease; Vaccines (Infectious Disease)",
        "Genitourinary", "Ophthalmology", "Vaccines (Infectious Disease)"
    }
    if colexists("Therapeutic Area"):
        df["Bain_Therapeutic Area"] = df["Therapeutic Area"].where(
            df["Therapeutic Area"].isin(whitelist), other="Multiple"
        )
    else:
        df["Bain_Therapeutic Area"] = "Multiple"

    # Collapse vaccine variants to 'Vaccines'
    vaccine_variants = {
        "Infectious Disease; Vaccines (Infectious Disease)",
        "Vaccines (Infectious Disease)"
    }
    df.loc[df["Bain_Therapeutic Area"].isin(vaccine_variants), "Bain_Therapeutic Area"] = "Vaccines"

    # Bain_Covid Tag
    title = df["Trial Title"].fillna("").astype(str) if colexists("Trial Title") else pd.Series("", index=df.index)
    dis   = df["Disease"].fillna("").astype(str) if colexists("Disease") else pd.Series("", index=df.index)
    joined_text = (title.str.upper() + " " + dis.str.upper())
    covid_tokens = [
        "COVID-19", "COVID", "2019-NCOV", "SARS-COV-2", "WUHAN CORONAVIRUS",
        "NCOV-19", "NCOV-2019", "2019 NOVEL CORONAVIRUS",
        "SEVERE ACUTE RESPIRATORY SYNDROME CORONAVIRUS 2"
    ]
    covid_mask = pd.Series(False, index=df.index)
    for token in covid_tokens:
        covid_mask |= joined_text.str.contains(token, regex=False)
    df["Bain_Covid Tag"] = covid_mask.map(lambda x: "Covid - Recommend Exclude" if x else "")

    # Bain_Phase mapping
    if colexists("Trial Phase"):
        raw_phase = df["Trial Phase"].fillna("").astype(str).str.upper().str.strip()
    else:
        raw_phase = pd.Series("", index=df.index)
    phase_map = {"I/II": "II", "II/III": "III", "III/IV": "III", "I": "I", "II": "II", "III": "III", "IV": "IV"}
    df["Bain_Phase"] = raw_phase.map(phase_map).fillna("Recommend Exclude")

    # Bain_Trial Region bucketing
    tr = df["Trial Region"].fillna("").astype(str) if colexists("Trial Region") else pd.Series("", index=df.index)
    has_na   = tr.str.contains("North America", regex=False)
    has_eu   = tr.str.contains("Western Europe", regex=False)
    has_asia = tr.str.contains("Asia", regex=False)
    has_au   = tr.str.contains("Australia/Oceania", regex=False)
    has_apac = has_asia | has_au

    df["Bain_Trial Region"] = "Other"
    df.loc[ has_na & ~has_eu & ~has_apac, "Bain_Trial Region"] = "NA only"
    df.loc[ has_eu & ~has_na & ~has_apac, "Bain_Trial Region"] = "EU only"
    df.loc[ has_apac & ~has_na & ~has_eu, "Bain_Trial Region"] = "APAC only"
    df.loc[ has_na & has_eu & ~has_apac,  "Bain_Trial Region"] = "NA and EU"
    df.loc[ has_na & has_apac & ~has_eu,  "Bain_Trial Region"] = "NA and APAC"
    df.loc[ has_eu & has_apac & ~has_na,  "Bain_Trial Region"] = "EU and APAC"
    df.loc[ has_na & has_eu & has_apac,   "Bain_Trial Region"] = "Global"

    # Filter Start Date > Last Modified Date (keep rows where Start < Last Modified)
    if colexists("Start Date") and colexists("Last Modified Date"):
        df = df[df["Start Date"] <= df["Last Modified Date"]]

    # Bain_Healthy Patient
    title_u   = (df["Trial Title"].fillna("").astype(str).str.upper() if colexists("Trial Title") else pd.Series("", index=df.index))
    keywords_u= (df["Study Keywords"].fillna("").astype(str).str.upper() if colexists("Study Keywords") else pd.Series("", index=df.index))
    design_u  = (df["TT_Study Design"].fillna("").astype(str).str.upper() if colexists("TT_Study Design") else pd.Series("", index=df.index))
    hp_regex  = "|".join(["HEALTHY", "BIOEQUIVALENCE", "BIOAVAILABILITY"])
    is_hp_text = title_u.str.contains(hp_regex, regex=True) | keywords_u.str_contains(hp_regex, regex=True) if hasattr(pd.Series.str, 'contains') else (
        title_u.str.contains(hp_regex, regex=True) | keywords_u.str.contains(hp_regex, regex=True)
    )
    is_hp_text = is_hp_text | design_u.str.contains(hp_regex, regex=True)
    df["Bain_Healthy Patient"] = (
        (df.get("NCT Code", pd.Series("", index=df.index)) == "No NCT Code") &
        # (df["Bain_Phase"] == "I") &
        is_hp_text.fillna(False)
    ).map(lambda x: "Yes" if x else "No")

    # ---------------- dynamic filters from `filters` ----------------

    # Trial statuses
    if colexists("TT_Trial Status") and is_effective_list(selected_trial_statuses):
        df = df[df["TT_Trial Status"].isin(selected_trial_statuses)]

    # Phases (use Bain_Phase)
    if is_effective_list(selected_phases):
        df = df[df["Bain_Phase"].isin(selected_phases)]

    # Study designs
    # if colexists("TT_Study Design") and is_effective_list(selected_study_designs):
    #     sel = [str(x).upper() for x in selected_study_designs]
    #     df = df[df["TT_Study Design"].fillna("").astype(str).str.upper().isin(sel)]

    # Therapeutic areas (use Bain_Therapeutic Area)
    if is_effective_list(selected_therapeutic_areas):
        df = df[df["Bain_Therapeutic Area"].isin(selected_therapeutic_areas)]

    # Sponsor/Collaborator Type (prefix match e.g., 'Industry')
    if colexists("Bain_Cleaned Sponsor/Collaborator Type") and is_effective_list(selected_lead_sponsor_types, all_tokens=("ALL", "ANY")):
        df = df[df["Bain_Cleaned Sponsor/Collaborator Type"].isin(selected_lead_sponsor_types)]

    # Specific sponsors
    if colexists("Bain_Lead Sponsor") and is_effective_list(selected_sponsors, all_tokens=("ALL", "ALL SPONSORS INCLUDED", "ANY")):
        df = df[df["Bain_Lead Sponsor"].isin(selected_sponsors)]

    # Region filter (only if enabled; 'Global' means no filtering)
    if region_enabled and region_value.strip().upper() != "GLOBAL":
        df = df[df["Bain_Trial Region"] == region_value]

    return df
