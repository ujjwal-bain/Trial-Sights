from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from datetime import datetime, timedelta
import time
import uuid
from filtering import apply_filters

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'

# Default filter values
DEFAULT_FILTERS = {
    'start_date_from_month': 1,
    'start_date_from_year': 2015,
    'start_date_until_month': datetime.now().month - 2 if datetime.now().month > 2 else 12,
    'start_date_until_year': datetime.now().year if datetime.now().month > 2 else datetime.now().year - 1,
    'region_enabled': False,
    'region': 'Global',
    'selected_phases': ['I', 'I/II', 'II', 'II/III', 'III', 'III/IV', 'IV'],
    'selected_study_designs': ['Interventional'],
    'selected_trial_statuses': ['Open', 'Closed', 'Temporarily Closed'],
    'selected_lead_sponsor_types': ['Industry'],
    'therapeutic_area_enabled': False,
    'selected_therapeutic_areas': ['Multiple'],
    'sponsor_enabled': False,
    'selected_sponsors': ['All Sponsors included'],
    'data_source': 'ClinicalTrials.gov only',
    'csv_file': None,
    'scale_up_factor': 0
}

@app.route('/')
def landing():
    return render_template('landing.html')

@app.route('/filters')
def filters():
    # Initialize session with default filters if not present
    if 'filters' not in session:
        session['filters'] = DEFAULT_FILTERS.copy()
    
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

@app.route('/update_filters', methods=['POST'])
def update_filters():
    # Update session filters with form data
    filters = session.get('filters', DEFAULT_FILTERS.copy())
    
    # Update basic fields
    filters['start_date_from_month'] = int(request.form.get('start_date_from_month', 1))
    filters['start_date_from_year'] = int(request.form.get('start_date_from_year', 2015))
    filters['start_date_until_month'] = int(request.form.get('start_date_until_month', 1))
    filters['start_date_until_year'] = int(request.form.get('start_date_until_year', 2015))
    
    filters['region_enabled'] = 'region_enabled' in request.form
    filters['region'] = request.form.get('region', 'Global')
    
    # filters['therapeutic_area_enabled'] = 'therapeutic_area_enabled' in request.form
    # filters['sponsor_enabled'] = 'sponsor_enabled' in request.form
    
    filters['data_source'] = request.form.get('data_source', 'ClinicalTrials.gov only')
    filters['scale_up_factor'] = int(request.form.get('scale_up_factor', 0))
    
    if 'csv_file' in request.files:
        csv_file = request.files['csv_file']
        if csv_file.filename != '':
            filters['csv_file'] = csv_file.filename
    
    # Handle multi-select fields
    filters['selected_phases'] = request.form.getlist('selected_phases')
    filters['selected_study_designs'] = request.form.getlist('selected_study_designs')
    filters['selected_trial_statuses'] = request.form.getlist('selected_trial_statuses')
    filters['selected_lead_sponsor_types'] = request.form.getlist('selected_lead_sponsor_types')
    filters['selected_therapeutic_areas'] = request.form.getlist('selected_therapeutic_areas')
    filters['selected_sponsors'] = request.form.getlist('selected_sponsors')
    
    session['filters'] = filters

    return redirect(url_for('filters'))

@app.route('/reset_filters', methods=['POST'])
def reset_filters():
    session['filters'] = DEFAULT_FILTERS.copy()
    return redirect(url_for('filters'))

@app.route('/submit_request', methods=['POST'])
def submit_request():
    # Generate a unique request ID
    request_id = str(uuid.uuid4())[:8]
    session['request_id'] = request_id
    session['request_time'] = datetime.now()
    session['analysis_status'] = 'processing'

    # apply_filters(input, filters)

    filters = session.get('filters')
    print("All filters stored in session:", filters)
    
    return redirect(url_for('results'))

@app.route('/results')
def results():
    if 'request_id' not in session:
        return redirect(url_for('filters'))
    
    request_time = session.get('request_time', datetime.now())
    request_id = session.get('request_id', '12345')
    
    return render_template('results.html', 
                         request_time=request_time,
                         request_id=request_id)

@app.route('/api/progress')
def get_progress():
    # Simulate progress for the analysis
    session['request_time'] = datetime.now().timestamp()

    if 'request_time' not in session:
        return jsonify({'progress': 0, 'status': 'processing', 'step': 'Initializing...'})
    
    request_time = session['request_time']
    elapsed = datetime.now().timestamp() - request_time
    
    steps = [
        'Initializing analysis...',
        'Filtering trial data...',
        'Applying regional filters...',
        'Processing phase criteria...',
        'Calculating scale-up factors...',
        'Generating insights...',
        'Finalizing results...'
    ]
    
    # Simulate 2-3 minute processing time
    total_time = 150  # 2.5 minutes
    progress = min(100, (elapsed / total_time) * 100)
    
    if progress >= 100:
        status = 'ready'
        step = 'Analysis complete!'
    else:
        status = 'processing'
        step_index = min(len(steps) - 1, int((progress / 100) * len(steps)))
        step = steps[step_index]
    
    return jsonify({
        'progress': int(progress),
        'status': status,
        'step': step
    })

if __name__ == '__main__':
    app.run(debug=True)
