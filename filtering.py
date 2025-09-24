import pandas as pd

def apply_filters(
    df: pd.DataFrame,
    *,
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
) -> pd.DataFrame:
    """
    Transforms and filters the trials DataFrame.
    Start month/year is inclusive; end month/year is exclusive.
    """

    df = df.copy()

    # filter out Planned
    df = df[df["TT_Trial Status"].fillna("") != "Planned"]

    # coerce dates
    df["Start Date"] = pd.to_datetime(df["Start Date"], errors="coerce", utc=True)
    df["Last Modified Date"] = pd.to_datetime(df["Last Modified Date"], errors="coerce", utc=True)

    # derive start year/month columns
    df["Bain_Start Year"] = df["Start Date"].dt.year.astype("Int64")
    df["Bain_Start Month"] = df["Start Date"].dt.month.astype("Int64")

    # month-aware window: [start, end)
    start_bound = pd.Timestamp(start_year, start_month, 1, tz="UTC")
    end_bound   = pd.Timestamp(end_year,   end_month,   1, tz="UTC")
    df = df[(df["Start Date"] >= start_bound) & (df["Start Date"] < end_bound)]

    # Bain_Therapeutic Area (whitelist else 'Multiple')
    whitelist = {
        "Oncology", "Cns", "Unassigned", "Metabolic/Endocrinology", "Autoimmune/Inflammation",
        "Infectious Disease", "Cardiovascular", "Infectious Disease; Vaccines (Infectious Disease)",
        "Genitourinary", "Ophthalmology", "Vaccines (Infectious Disease)"
    }

    df["Bain_Therapeutic Area"] = df["Therapeutic Area"].where(
        df["Therapeutic Area"].isin(whitelist), other="Multiple"
    )

    # Collapse vaccine variants to 'Vaccines'
    vaccine_variants = {
        "Infectious Disease; Vaccines (Infectious Disease)",
        "Vaccines (Infectious Disease)"
    }
    df.loc[df["Bain_Therapeutic Area"].isin(vaccine_variants), "Bain_Therapeutic Area"] = "Vaccines"

    # Bain_Covid Tag
    covid_tokens = [
        "COVID-19", "COVID", "2019-NCOV", "SARS-COV-2", "WUHAN CORONAVIRUS",
        "NCOV-19", "NCOV-2019", "2019 NOVEL CORONAVIRUS",
        "SEVERE ACUTE RESPIRATORY SYNDROME CORONAVIRUS 2"
    ]
    joined_text = (
        df["Trial Title"].fillna("").astype(str).str.upper() + " " +
        df["Disease"].fillna("").astype(str).str.upper()
    )
    covid_mask = pd.Series(False, index=df.index)
    for token in covid_tokens:
        covid_mask = covid_mask | joined_text.str.contains(token, regex=False)
    df["Bain_Covid Tag"] = covid_mask.map(lambda x: "Covid - Recommend Exclude" if x else "")

    # Bain_Phase mapping
    raw_phase = df["Trial Phase"].fillna("").astype(str).str.upper().str.strip()
    phase_map = {
        "I/II": "II",
        "II/III": "III",
        "III/IV": "III",
        "I": "I",
        "II": "II",
        "III": "III",
        "IV": "IV",
    }
    df["Bain_Phase"] = raw_phase.map(phase_map).fillna("Recommend Exclude")

    # Bain_Trial Region bucketing
    tr = df["Trial Region"].fillna("").astype(str)
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

    # Filter Start Date > Last Modified Date
    df = df[df["Start Date"] < df["Last Modified Date"]]

    # Bain_Healthy Patient
    title_u   = df["Trial Title"].fillna("").astype(str).str.upper()
    keywords_u= df["Study Keywords"].fillna("").astype(str).str.upper()
    design_u  = df["TT_Study Design"].fillna("").astype(str).str.upper()
    hp_tokens = ["HEALTHY", "BIOEQUIVALENCE", "BIOAVAILABILITY"]
    hp_regex  = "|".join(hp_tokens)
    is_hp_text = (
        title_u.str.contains(hp_regex, regex=True) |
        keywords_u.str.contains(hp_regex, regex=True) |
        design_u.str.contains(hp_regex, regex=True)
    )
    df["Bain_Healthy Patient"] = (
        (df["NCT Code"] == "No NCT Code") &
        (df["Bain_Phase"] == "I") &
        is_hp_text
    ).map(lambda x: "Yes" if x else "No")

    # Filter Sponsor/Collaborator Type starts with 'Industry'
    sct = df["Sponsor/Collaborator Type"].fillna("").astype(str)
    df = df[sct.str.startswith("Industry")]

    return df

# input = pd.read_csv("database/temp.csv", encoding='latin1')
# output = process_trials_df(input, start_year=2023, start_month=1, end_year=2024, end_month=1)
# output.to_csv("database/processed_temp.csv", index=False)