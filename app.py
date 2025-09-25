import argparse
from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from datetime import datetime, timedelta
import time
import uuid
from filtering import apply_filters
from functools import lru_cache
import pandas as pd
from TT_Read_Clean import TT_Cleaning
from CT_GOV_Read_Clean import CT_GOV_Cleaning
from Join_Union import run_join_operation
from Lead_Sponsor import add_lead_sponsor
from Revenue_Mapping import map_revenue

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'

# Optional: centralize file paths (so your cleaning funcs can take arguments if you add them)
app.config.update(
    TT_EXCEL_PATH = r"C:\Users\61272\OneDrive - Bain\Documents\Work\IP\TrialTrove Health Sector IP\Abbvie_Trialtove_Sample data.xlsx",
    TT_EXCEL_SHEET = "Results",
    TT_OUTPUT_PATH   = r"C:\Users\61272\OneDrive - Bain\Documents\Work\IP\TrialTrove Health Sector IP\TT_Python Clean Sample.xlsx",
    CT_CSV_PATH   = r"C:\Users\61272\OneDrive - Bain\Documents\Work\IP\TrialTrove Health Sector IP\CT.gov data for workflow 276K.csv",
    CT_OUTPUT_PATH = r"C:\Users\61272\OneDrive - Bain\Documents\Work\IP\TrialTrove Health Sector IP\CT Gov_Python Clean Sample.xlsx",
    MERGE_LEFT_PATH = r"C:\Users\61272\OneDrive - Bain\Documents\Work\IP\TrialTrove Health Sector IP\Left_only_TT_CT.xlsx",
    MERGE_JOIN_PATH = r"C:\Users\61272\OneDrive - Bain\Documents\Work\IP\TrialTrove Health Sector IP\Join_TT_CT.xlsx",
    MERGE_UNION_PATH = r"C:\Users\61272\OneDrive - Bain\Documents\Work\IP\TrialTrove Health Sector IP\Union_TT_CT.xlsx",
    LEAD_SPONSOR_PATH = r"C:\Users\61272\OneDrive - Bain\Documents\Work\IP\TrialTrove Health Sector IP\Union_with_Lead_Sponsor.xlsx",
    REV_MAP1_PATH     = r"C:\Users\61272\OneDrive - Bain\Documents\Work\IP\TrialTrove Health Sector IP\Mapping table Bain_Lead Sponsor to EP Name.xlsx",
    REV_MAP1_SHEET    = 0,   # or "Sheet1"
    REV_US_PATH       = r"C:\Users\61272\OneDrive - Bain\Documents\Work\IP\TrialTrove Health Sector IP\Mapping table_EP 2023 global pharma vs. SMID US.xlsx",
    REV_US_SHEET      = 0,
    REV_WW_PATH       = r"C:\Users\61272\OneDrive - Bain\Documents\Work\IP\TrialTrove Health Sector IP\Mapping table_EP 2024 global pharma vs. SMID WW.xlsx",
    REV_WW_SHEET      = 0,
    REV_OUTPUT_PATH   = r"C:\Users\61272\OneDrive - Bain\Documents\Work\IP\TrialTrove Health Sector IP\Revenue Mapping using files.xlsx")

@lru_cache(maxsize=1)
def load_clean_data() -> tuple:
    #    Runs the cleaning once per process and caches the pair of DataFrames. In dev, the debug reloader creates a second process; each gets its own cache
    tt_df = TT_Cleaning(excel_path=app.config['TT_EXCEL_PATH'], sheet=app.config['TT_EXCEL_SHEET'], output_path=app.config.get('TT_OUTPUT_PATH'))
    ct_df = CT_GOV_Cleaning(csv_path=app.config['CT_CSV_PATH'], output_path=app.config.get('CT_OUTPUT_PATH'))
    return tt_df, ct_df

# @app.route('/filters')
# def filters():
#     # Initialize session with default filters if not present
#     if 'filters' not in session:
#         session['filters'] = DEFAULT_FILTERS.copy()
    
    # Generate months and years for dropdowns
    months = [
        {'value': 1, 'label': 'January'}, {'value': 2, 'label': 'February'},
        {'value': 3, 'label': 'March'}, {'value': 4, 'label': 'April'},
        {'value': 5, 'label': 'May'}, {'value': 6, 'label': 'June'},
        {'value': 7, 'label': 'July'}, {'value': 8, 'label': 'August'},
        {'value': 9, 'label': 'September'}, {'value': 10, 'label': 'October'},
        {'value': 11, 'label': 'November'}, {'value': 12, 'label': 'December'}
    ]
    
    current_year = datetime.now().year
    years = list(range(2015, current_year + 1))
    
    # Available options
    phases = ['I', 'I/II', 'II', 'II/III', 'III', 'III/IV', 'IV', 'Others']
    study_designs = ['Interventional', 'Observational']
    trial_statuses = ['Open', 'Closed', 'Temporarily Closed', 'Completed', 'Planned', 'Terminated']
    lead_sponsor_types = ['Industry', 'Academic', 'Other']
    
    therapeutic_areas = [
        'Multiple', 'Oncology', 'CNS', 'Unassigned', 'Metabolic/Endocrinology',
        'Autoimmune/Inflammation', 'Infectious Disease', 'Cardiovascular',
        'Infectious Disease; Vaccines (Infectious Disease)', 'Genitourinary',
        'Ophthalmology', 'Vaccines (Infectious Disease)'
    ]
    
    major_sponsors = [
        'All Sponsors included', 'Pfizer Inc.', 'Novartis Pharmaceuticals',
        'Roche/Genentech', 'Johnson & Johnson', 'Merck & Co.',
        'Bristol Myers Squibb', 'AstraZeneca', 'Sanofi', 'GlaxoSmithKline',
        'Eli Lilly and Company', 'AbbVie Inc.', 'Amgen Inc.',
        'Gilead Sciences', 'Biogen Inc.', 'Regeneron Pharmaceuticals'
    ]
    
    data_sources = [
        'ClinicalTrials.gov only',
        'CT + TT (Phase 1)',
        'CT + TT (Industry)',
        'Citeline TrialTrove only'
    ]
    
    regions = [
        {'value': 'Global', 'label': 'Global'},
        {'value': 'na', 'label': 'North America only'},
        {'value': 'eu', 'label': 'European Union only'},
        {'value': 'apac', 'label': 'APAC only'},
        {'value': 'euapac', 'label': 'EU and APAC'},
        {'value': 'naapac', 'label': 'North America and APAC'}
    ]
    
    return render_template('filters.html', 
                         filters=session['filters'],
                         months=months,
                         years=years,
                         phases=phases,
                         study_designs=study_designs,
                         trial_statuses=trial_statuses,
                         lead_sponsor_types=lead_sponsor_types,
                         therapeutic_areas=therapeutic_areas,
                         major_sponsors=major_sponsors,
                         data_sources=data_sources,
                         regions=regions,
                         current_year=datetime.now().year,
                         current_month=datetime.now().month)


def _get_opt_bool(name: str):
    """
    Returns:
      True  -> if value looks truthy ("1","true","yes","on","t")
      False -> if value looks falsy  ("0","false","no","off","f")
      None  -> if the param/key is missing (checkbox was not sent)
    Works with query string, form, or JSON body.
    """
    # query string or form
    v = (request.args.get(name) or request.form.get(name))
    if v is None:
        # try JSON
        data = request.get_json(silent=True) or {}
        v = data.get(name)
        if v is None:
            return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("1", "true", "t", "yes", "on"):   return True
    if s in ("0", "false", "f", "no", "off"):  return False
    return None

@app.route("/run", methods=["GET"])
def run_pipeline():
    tt_df, ct_df = load_clean_data()
    study_interventional = _get_opt_bool("interventional")
    study_observational  = _get_opt_bool("observational")
    sponsor_industry  = _get_opt_bool("sponsor_industry")
    sponsor_academic  = _get_opt_bool("sponsor_academic")
    sponsor_others    = _get_opt_bool("sponsor_others")
    
    # Update basic fields
    # filters['start_date_from_month'] = int(request.form.get('start_date_from_month', 1))
    # filters['start_date_from_year'] = int(request.form.get('start_date_from_year', 2015))
    # filters['start_date_until_month'] = int(request.form.get('start_date_until_month', 1))
    # filters['start_date_until_year'] = int(request.form.get('start_date_until_year', 2015))
    
    # filters['region_enabled'] = 'region_enabled' in request.form
    # filters['region'] = request.form.get('region', 'Global')
    
    # # filters['therapeutic_area_enabled'] = 'therapeutic_area_enabled' in request.form
    # # filters['sponsor_enabled'] = 'sponsor_enabled' in request.form
    
    # filters['data_source'] = request.form.get('data_source', 'ClinicalTrials.gov only')
    # filters['scale_up_factor'] = int(request.form.get('scale_up_factor', 0))
    
    # if 'csv_file' in request.files:
    #     csv_file = request.files['csv_file']
    #     if csv_file.filename != '':
    #         filters['csv_file'] = csv_file.filename
    
    # # Handle multi-select fields
    # filters['selected_phases'] = request.form.getlist('selected_phases')
    # filters['selected_study_designs'] = request.form.getlist('selected_study_designs')
    # filters['selected_trial_statuses'] = request.form.getlist('selected_trial_statuses')
    # filters['selected_lead_sponsor_types'] = request.form.getlist('selected_lead_sponsor_types')
    # filters['selected_therapeutic_areas'] = request.form.getlist('selected_therapeutic_areas')
    # filters['selected_sponsors'] = request.form.getlist('selected_sponsors')
    
    # session['filters'] = filters

    # return redirect(url_for('filters'))
    Left_only_TT_CT, Join_TT_CT, Union_TT_CT = run_join_operation(
        tt_df,
        ct_df,
        right_cols_to_keep=None,  # use defaults in Join_Union; or pass a list to override
        suffix_for_ct="_CT",
        output_left_path=app.config.get("MERGE_LEFT_PATH"),
        output_join_path=app.config.get("MERGE_JOIN_PATH"),
        output_union_path=app.config.get("MERGE_UNION_PATH"),
        debug=True, study_interventional=study_interventional, study_observational=study_observational,
        sponsor_industry=sponsor_industry, sponsor_academic=sponsor_academic, sponsor_others=sponsor_others)

    union_with_lead = add_lead_sponsor(Union_TT_CT, output_path=app.config.get("LEAD_SPONSOR_PATH"))

# @app.route('/submit_request', methods=['POST'])
# def submit_request():
#     # Generate a unique request ID
#     request_id = str(uuid.uuid4())[:8]
#     session['request_id'] = request_id
#     session['request_time'] = datetime.now()
#     session['analysis_status'] = 'processing'

#     # apply_filters(input, filters)

#     filters = session.get('filters')
#     print("All filters stored in session:", filters)
    
#     return redirect(url_for('results'))

# @app.route('/results')
# def results():
#     if 'request_id' not in session:
#         return redirect(url_for('filters'))
    
#     request_time = session.get('request_time', datetime.now())
#     request_id = session.get('request_id', '12345')
    
#     return render_template('results.html', 
#                          request_time=request_time,
#                          request_id=request_id)

# @app.route('/api/progress')
# def get_progress():
#     # Simulate progress for the analysis
#     session['request_time'] = datetime.now().timestamp()

#     if 'request_time' not in session:
#         return jsonify({'progress': 0, 'status': 'processing', 'step': 'Initializing...'})
    
#     request_time = session['request_time']
#     elapsed = datetime.now().timestamp() - request_time
    
#     steps = [
#         'Initializing analysis...',
#         'Filtering trial data...',
#         'Applying regional filters...',
#         'Processing phase criteria...',
#         'Calculating scale-up factors...',
#         'Generating insights...',
#         'Finalizing results...'
#     ]
    
#     # Simulate 2-3 minute processing time
#     total_time = 150  # 2.5 minutes
#     progress = min(100, (elapsed / total_time) * 100)
    
#     if progress >= 100:
#         status = 'ready'
#         step = 'Analysis complete!'
#     else:
#         status = 'processing'
#         step_index = min(len(steps) - 1, int((progress / 100) * len(steps)))
#         step = steps[step_index]
    
    rev_df = map_revenue(
        union_with_lead,
        map1_path=app.config["REV_MAP1_PATH"],
        map1_sheet=app.config.get("REV_MAP1_SHEET", 0),
        map_us_path=app.config["REV_US_PATH"],
        map_us_sheet=app.config.get("REV_US_SHEET", 0),
        map_ww_path=app.config["REV_WW_PATH"],
        map_ww_sheet=app.config.get("REV_WW_SHEET", 0),
        cleanse_inputs=app.config.get("REV_CLEANSE_INPUTS", True),
        remove_punct=app.config.get("REV_REMOVE_PUNCT", True),
        title_case=app.config.get("REV_TITLE_CASE", True),
        output_path=app.config.get("REV_OUTPUT_PATH"),
    )

    # next steps: merge/map/apply filters; for now just return sizes
    return jsonify({
        "tt_rows": len(tt_df),
        "tt_cols": len(tt_df.columns),
        "ct_rows": len(ct_df),
        "ct_cols": len(ct_df.columns),
        "left_rows": len(Left_only_TT_CT),
        "left_cols": len(Left_only_TT_CT.columns),
        "join_rows": len(Join_TT_CT),
        "join_cols": len(Join_TT_CT.columns),
        "union_rows": len(Union_TT_CT),
        "union_cols": len(Union_TT_CT.columns),
        "lead_rows": len(union_with_lead),
        "lead_cols": len(union_with_lead.columns),
        "rev_rows": len(rev_df),
        "rev_cols": len(rev_df.columns),
        "saved_left": bool(app.config.get("MERGE_LEFT_PATH")),
        "saved_join": bool(app.config.get("MERGE_JOIN_PATH")),
        "saved_union": bool(app.config.get("MERGE_UNION_PATH")),
        "saved_lead":  bool(app.config.get("LEAD_SPONSOR_PATH")),
        "interventional": study_interventional,
        "observational": study_observational,
        "sponsor_industry": sponsor_industry,
        "sponsor_academic": sponsor_academic,
        "sponsor_others":   sponsor_others})

# Example: use cleaned data when showing results
# @app.route("/results")
# def results():
#     # ensure you have whatever session state you want here
#     tt_df, ct_df = load_clean_data()
#     # TODO: apply filters / merge / compute summary
#     return render_template("results.html", request_time=datetime.now(), request_id="demo-123")



# # Default filter values
# DEFAULT_FILTERS = {
#     'start_date_from_month': 1,
#     'start_date_from_year': 2015,
#     'start_date_until_month': datetime.now().month - 2 if datetime.now().month > 2 else 12,
#     'start_date_until_year': datetime.now().year if datetime.now().month > 2 else datetime.now().year - 1,
#     'region_enabled': False,
#     'region': 'Global',
#     'selected_phases': ['I', 'I/II', 'II', 'II/III', 'III', 'III/IV', 'IV'],
#     'selected_study_designs': ['Interventional'],
#     'selected_trial_statuses': ['Open', 'Closed', 'Temporarily Closed'],
#     'selected_lead_sponsor_types': ['Industry'],
#     'therapeutic_area_enabled': False,
#     'selected_therapeutic_areas': ['Multiple'],
#     'sponsor_enabled': False,
#     'selected_sponsors': ['All Sponsors included'],
#     'data_source': 'ClinicalTrials.gov only',
#     'csv_file': None,
#     'scale_up_factor': 0
# }

# @app.route('/')
# def landing():
#     return render_template('landing.html')

# @app.route('/filters')
# def filters():
#     # Initialize session with default filters if not present
#     if 'filters' not in session:
#         session['filters'] = DEFAULT_FILTERS.copy()
    
#     # Generate months and years for dropdowns
#     months = [
#         {'value': 1, 'label': 'January'}, {'value': 2, 'label': 'February'},
#         {'value': 3, 'label': 'March'}, {'value': 4, 'label': 'April'},
#         {'value': 5, 'label': 'May'}, {'value': 6, 'label': 'June'},
#         {'value': 7, 'label': 'July'}, {'value': 8, 'label': 'August'},
#         {'value': 9, 'label': 'September'}, {'value': 10, 'label': 'October'},
#         {'value': 11, 'label': 'November'}, {'value': 12, 'label': 'December'}
#     ]
    
#     current_year = datetime.now().year
#     years = list(range(2015, current_year + 1))
    
#     # Available options
#     phases = ['I', 'I/II', 'II', 'II/III', 'III', 'III/IV', 'IV', 'Others']
#     study_designs = ['Interventional', 'Observational']
#     trial_statuses = ['Open', 'Closed', 'Temporarily Closed', 'Completed', 'Terminated']
#     lead_sponsor_types = ['Industry', 'Non-Industry', 'Unknown']
    
#     therapeutic_areas = [
#         'Multiple', 'Oncology', 'CNS', 'Unassigned', 'Metabolic/Endocrinology',
#         'Autoimmune/Inflammation', 'Infectious Disease', 'Cardiovascular',
#         'Infectious Disease; Vaccines (Infectious Disease)', 'Genitourinary',
#         'Ophthalmology', 'Vaccines (Infectious Disease)'
#     ]
    
#     major_sponsors = [
#         'All Sponsors included', 'Pfizer Inc.', 'Novartis Pharmaceuticals',
#         'Roche/Genentech', 'Johnson & Johnson', 'Merck & Co.',
#         'Bristol Myers Squibb', 'AstraZeneca', 'Sanofi', 'GlaxoSmithKline',
#         'Eli Lilly and Company', 'AbbVie Inc.', 'Amgen Inc.',
#         'Gilead Sciences', 'Biogen Inc.', 'Regeneron Pharmaceuticals'
#     ]
    
#     data_sources = [
#         'ClinicalTrials.gov only',
#         'CT + TT (Phase 1)',
#         'CT + TT (Industry)',
#         'Citeline TrialTrove only'
#     ]
    
#     regions = [
#         {'value': 'Global', 'label': 'Global'},
#         {'value': 'na', 'label': 'North America only'},
#         {'value': 'eu', 'label': 'European Union only'},
#         {'value': 'apac', 'label': 'APAC only'},
#         {'value': 'euapac', 'label': 'EU and APAC'},
#         {'value': 'naapac', 'label': 'North America and APAC'}
#     ]
    
#     return render_template('filters.html', 
#                          filters=session['filters'],
#                          months=months,
#                          years=years,
#                          phases=phases,
#                          study_designs=study_designs,
#                          trial_statuses=trial_statuses,
#                          lead_sponsor_types=lead_sponsor_types,
#                          therapeutic_areas=therapeutic_areas,
#                          major_sponsors=major_sponsors,
#                          data_sources=data_sources,
#                          regions=regions,
#                          current_year=datetime.now().year,
#                          current_month=datetime.now().month)

# @app.route('/update_filters', methods=['POST'])
# def update_filters():
#     # Update session filters with form data
#     filters = session.get('filters', DEFAULT_FILTERS.copy())
    
#     # Update basic fields
#     filters['start_date_from_month'] = int(request.form.get('start_date_from_month', 1))
#     filters['start_date_from_year'] = int(request.form.get('start_date_from_year', 2015))
#     filters['start_date_until_month'] = int(request.form.get('start_date_until_month', 1))
#     filters['start_date_until_year'] = int(request.form.get('start_date_until_year', 2015))
    
#     filters['region_enabled'] = 'region_enabled' in request.form
#     filters['region'] = request.form.get('region', 'Global')
    
#     filters['therapeutic_area_enabled'] = 'therapeutic_area_enabled' in request.form
#     filters['sponsor_enabled'] = 'sponsor_enabled' in request.form
    
#     filters['data_source'] = request.form.get('data_source', 'ClinicalTrials.gov only')
#     filters['scale_up_factor'] = int(request.form.get('scale_up_factor', 0))
    
#     if 'csv_file' in request.files:
#         csv_file = request.files['csv_file']
#         if csv_file.filename != '':
#             filters['csv_file'] = csv_file.filename
    
#     # Handle multi-select fields
#     filters['selected_phases'] = request.form.getlist('selected_phases')
#     filters['selected_study_designs'] = request.form.getlist('selected_study_designs')
#     filters['selected_trial_statuses'] = request.form.getlist('selected_trial_statuses')
#     filters['selected_lead_sponsor_types'] = request.form.getlist('selected_lead_sponsor_types')
#     filters['selected_therapeutic_areas'] = request.form.getlist('selected_therapeutic_areas')
#     filters['selected_sponsors'] = request.form.getlist('selected_sponsors')
    
#     session['filters'] = filters
#     return redirect(url_for('filters'))

# @app.route('/reset_filters', methods=['POST'])
# def reset_filters():
#     session['filters'] = DEFAULT_FILTERS.copy()
#     return redirect(url_for('filters'))

# @app.route('/submit_request', methods=['POST'])
# def submit_request():
#     # Generate a unique request ID
#     request_id = str(uuid.uuid4())[:8]
#     session['request_id'] = request_id
#     session['request_time'] = datetime.now()
#     session['analysis_status'] = 'processing'
    
#     return redirect(url_for('results'))

# @app.route('/results')
# def results():
#     if 'request_id' not in session:
#         return redirect(url_for('filters'))
    
#     request_time = session.get('request_time', datetime.now())
#     request_id = session.get('request_id', '12345')
    
#     return render_template('results.html', 
#                          request_time=request_time,
#                          request_id=request_id)

# @app.route('/api/progress')
# def get_progress():
#     # Simulate progress for the analysis
#     if 'request_time' not in session:
#         return jsonify({'progress': 0, 'status': 'processing', 'step': 'Initializing...'})
    
#     request_time = session['request_time']
#     elapsed = (datetime.now() - request_time).total_seconds()
    
#     steps = [
#         'Initializing analysis...',
#         'Filtering trial data...',
#         'Applying regional filters...',
#         'Processing phase criteria...',
#         'Calculating scale-up factors...',
#         'Generating insights...',
#         'Finalizing results...'
#     ]
    
#     # Simulate 2-3 minute processing time
#     total_time = 150  # 2.5 minutes
#     progress = min(100, (elapsed / total_time) * 100)
    
#     if progress >= 100:
#         status = 'ready'
#         step = 'Analysis complete!'
#     else:
#         status = 'processing'
#         step_index = min(len(steps) - 1, int((progress / 100) * len(steps)))
#         step = steps[step_index]
    
#     return jsonify({
#         'progress': int(progress),
#         'status': status,
#         'step': step
#     })


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trial-Sights runner")
    parser.add_argument("--once", action="store_true", help="Run cleaners once and exit (no web server)")
    args = parser.parse_args() 

    if args.once:
        # create outputs, then exit
        from TT_Read_Clean import TT_Cleaning
        from CT_GOV_Read_Clean import CT_GOV_Cleaning
        from Join_Union import run_join_operation  # ensure imported in this scope too

        TT_Initial = TT_Cleaning(
            excel_path=app.config['TT_EXCEL_PATH'],
            sheet=app.config['TT_EXCEL_SHEET'],
            output_path=app.config.get('TT_OUTPUT_PATH'),
        )

        CT_GOV_Initial = CT_GOV_Cleaning(
            csv_path=app.config['CT_CSV_PATH'],
            output_path=app.config.get('CT_OUTPUT_PATH'),
        )

        # If you want to test the checkbox refinement here, set them explicitly;
        # otherwise leave as None to skip refinement.
        study_interventional = None   # or True/False
        study_observational  = None   # or True/False

        Left_only_TT_CT, Join_TT_CT, Union_TT_CT = run_join_operation(
            TT_Initial,
            CT_GOV_Initial,
            right_cols_to_keep=None,                  # or ["NCT ID","brief_title","overall_status","interventions"]
            suffix_for_ct="_CT",
            output_left_path=app.config.get("MERGE_LEFT_PATH"),
            output_join_path=app.config.get("MERGE_JOIN_PATH"),
            output_union_path=app.config.get("MERGE_UNION_PATH"),
            debug=True,
            study_interventional=study_interventional,
            study_observational=study_observational,
        )
        union_with_lead = add_lead_sponsor(Union_TT_CT,output_path=app.config.get("LEAD_SPONSOR_PATH"))

        rev_df = map_revenue(
            union_with_lead,
            map1_path=app.config["REV_MAP1_PATH"],
            map1_sheet=app.config.get("REV_MAP1_SHEET", 0),
            map_us_path=app.config["REV_US_PATH"],
            map_us_sheet=app.config.get("REV_US_SHEET", 0),
            map_ww_path=app.config["REV_WW_PATH"],
            map_ww_sheet=app.config.get("REV_WW_SHEET", 0),
            cleanse_inputs=app.config.get("REV_CLEANSE_INPUTS", True),
            remove_punct=app.config.get("REV_REMOVE_PUNCT", True),
            title_case=app.config.get("REV_TITLE_CASE", True),
            output_path=app.config.get("REV_OUTPUT_PATH"),
        )

        print(
            f"Done. TT rows={len(TT_Initial):,}, CT rows={len(CT_GOV_Initial):,} | "
            f"Left rows={len(Left_only_TT_CT):,}, Join rows={len(Join_TT_CT):,}"
        )
    else:
        app.run(debug=True, use_reloader=False)


# cd "C:\Users\61272\OneDrive - Bain\Documents\GitHub\Trial-Sights"
# py app.py --once

