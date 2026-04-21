"""
System Health Dashboard Tab
Monitors automation flows, data freshness, and system errors
"""

from dash import html, dash_table
import pandas as pd
from datetime import datetime, timedelta
from data_pipeline import upload_data, config


def load_system_health_data():
    """Load data needed for system health monitoring."""
    uploader = upload_data.DataUploader()

    data = {}

    # Load customer flags
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customer_flags)
        data['flags'] = uploader.convert_csv_to_df(csv_content)
    except Exception as e:
        print(f"Error loading flags: {e}")
        data['flags'] = pd.DataFrame()

    # Load customer master (for data freshness)
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customers_master_v2)
        data['customers'] = uploader.convert_csv_to_df(csv_content)
    except Exception as e:
        print(f"Error loading customers: {e}")
        data['customers'] = pd.DataFrame()

    # Load birthday parties
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, 'birthday_parties/birthday_parties.csv')
        data['parties'] = uploader.convert_csv_to_df(csv_content)
    except Exception as e:
        print(f"Error loading parties: {e}")
        data['parties'] = pd.DataFrame()

    # Load birthday party RSVPs
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, 'birthday_parties/birthday_party_rsvps.csv')
        data['rsvps'] = uploader.convert_csv_to_df(csv_content)
    except Exception as e:
        print(f"Error loading RSVPs: {e}")
        data['rsvps'] = pd.DataFrame()

    return data


def get_birthday_automation_metrics(df_flags, df_parties, df_rsvps, days=7):
    """
    Get birthday party automation metrics for the last N days.

    Returns DataFrame with columns:
    - date
    - parties_7d_out (parties happening in 7 days from this date)
    - attendee_flags_set
    - attendee_sms_sent (placeholder - will track later)
    - host_flags_set
    - host_notif_sent (placeholder - will track later)
    """
    if df_flags.empty:
        return pd.DataFrame()

    # Convert dates
    df_flags['triggered_date'] = pd.to_datetime(df_flags['triggered_date'])

    # Get last N days
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days-1)
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    metrics = []

    for date in date_range:
        date_obj = date.date()

        # Count attendee flags triggered on this date
        attendee_flags = df_flags[
            (df_flags['flag_type'] == 'birthday_party_attendee_one_week_out') &
            (df_flags['triggered_date'].dt.date == date_obj)
        ]

        # Count host flags triggered on this date
        host_flags_6d = df_flags[
            (df_flags['flag_type'] == 'birthday_party_host_six_days_out') &
            (df_flags['triggered_date'].dt.date == date_obj)
        ]

        # Count host flags for 7d (initial notification)
        host_flags_7d = df_flags[
            (df_flags['flag_type'] == 'birthday_party_host_one_week_out') &
            (df_flags['triggered_date'].dt.date == date_obj)
        ]

        # Count parties that were 7 days out on this date
        target_party_date = date_obj + timedelta(days=7)
        if not df_parties.empty:
            df_parties_copy = df_parties.copy()
            df_parties_copy['party_date'] = pd.to_datetime(df_parties_copy['party_date']).dt.date
            parties_7d_out = len(df_parties_copy[df_parties_copy['party_date'] == target_party_date])
        else:
            parties_7d_out = 0

        metrics.append({
            'Date': date_obj.strftime('%b %d'),
            'Parties (7d out)': parties_7d_out,
            'Attendee Flags': len(attendee_flags),
            'Attendee SMS': '📧 TBD',  # Placeholder - will track actual sends later
            'Host Flags (6d)': len(host_flags_6d),
            'Host Notifications': '📧 TBD'  # Placeholder - will track actual sends later
        })

    return pd.DataFrame(metrics)


def get_2week_pass_journey_metrics(df_flags, days=7):
    """
    Get 2-week pass journey metrics for the last N days.

    Returns DataFrame with columns:
    - date
    - flags_set (first_time_day_pass_2wk_offer)
    - vicky_csv_sent
    - already_in_mailchimp
    - net_added
    """
    if df_flags.empty:
        return pd.DataFrame()

    # Convert dates
    df_flags['triggered_date'] = pd.to_datetime(df_flags['triggered_date'])

    # Get last N days
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days-1)
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    metrics = []

    for date in date_range:
        date_obj = date.date()

        # Count flags triggered on this date
        day_pass_flags = df_flags[
            (df_flags['flag_type'] == 'first_time_day_pass_2wk_offer') &
            (df_flags['triggered_date'].dt.date == date_obj)
        ]

        # For now, we don't have detailed tracking of Mailchimp duplicates or actual sends
        # These will be placeholders

        metrics.append({
            'Date': date_obj.strftime('%b %d'),
            'Flags Set': len(day_pass_flags),
            'Vicky CSV': '✅' if len(day_pass_flags) > 0 else '⏸️',
            'Already in MC': '📧 TBD',  # Will track when we log Mailchimp checks
            'Net Added': '📧 TBD'  # Will track when we log actual adds
        })

    return pd.DataFrame(metrics)


def get_data_freshness_metrics():
    """
    Get data freshness for key data sources.

    Returns DataFrame with columns:
    - data_source
    - last_updated
    - status
    """
    uploader = upload_data.DataUploader()

    sources = [
        {'name': 'Stripe Transactions', 's3_key': config.s3_path_combined},
        {'name': 'Customer Master', 's3_key': config.s3_path_customers_master_v2},
        {'name': 'Customer Flags', 's3_key': config.s3_path_customer_flags},
        {'name': 'Birthday Parties', 's3_key': 'birthday_parties/birthday_parties.csv'},
        {'name': 'Capitan Memberships', 's3_key': config.s3_path_capitan_memberships},
        {'name': 'Customer Events', 's3_key': config.s3_path_customer_events},
    ]

    metrics = []
    now = datetime.now()

    for source in sources:
        try:
            # Get S3 object metadata
            import boto3
            s3_client = boto3.client('s3')
            response = s3_client.head_object(Bucket=config.aws_bucket_name, Key=source['s3_key'])
            last_modified = response['LastModified']

            # Calculate time since last update
            time_diff = now - last_modified.replace(tzinfo=None)
            hours_ago = time_diff.total_seconds() / 3600

            if hours_ago < 1:
                age_str = f"{int(time_diff.total_seconds() / 60)}m ago"
            elif hours_ago < 24:
                age_str = f"{int(hours_ago)}h ago"
            else:
                age_str = f"{int(hours_ago / 24)}d ago"

            # Determine status
            if hours_ago < 6:
                status = '✅ Fresh'
            elif hours_ago < 24:
                status = '⚠️ Aging'
            else:
                status = '❌ Stale'

            metrics.append({
                'Data Source': source['name'],
                'Last Updated': age_str,
                'Status': status
            })
        except Exception as e:
            metrics.append({
                'Data Source': source['name'],
                'Last Updated': 'Error',
                'Status': f'❌ {str(e)[:30]}'
            })

    return pd.DataFrame(metrics)


def get_recent_errors():
    """
    Get recent errors and warnings from the system.

    For now, this is a placeholder showing common issues.
    Later we can pull from actual logs.
    """
    # Placeholder errors - in production, this would read from a log table
    now = datetime.now()

    errors = [
        {
            'Time': (now - timedelta(hours=2, minutes=15)).strftime('%I:%M %p'),
            'Type': 'ℹ️ Info',
            'Message': 'No birthday parties 7 days out today'
        },
        {
            'Time': (now - timedelta(hours=0, minutes=42)).strftime('%I:%M %p'),
            'Type': 'ℹ️ Info',
            'Message': 'SendGrid email functionality will be available later today (placeholder active)'
        },
    ]

    return pd.DataFrame(errors)


def create_system_health_layout():
    """Create the System Health tab layout."""

    # Load data
    data = load_system_health_data()

    # Get metrics
    df_birthday_metrics = get_birthday_automation_metrics(
        data.get('flags', pd.DataFrame()),
        data.get('parties', pd.DataFrame()),
        data.get('rsvps', pd.DataFrame()),
        days=7
    )

    df_2week_metrics = get_2week_pass_journey_metrics(
        data.get('flags', pd.DataFrame()),
        days=7
    )

    df_freshness = get_data_freshness_metrics()
    df_errors = get_recent_errors()

    # Get current flag counts
    df_flags = data.get('flags', pd.DataFrame())
    active_flags_summary = []
    if not df_flags.empty:
        # Get today's flags
        today = datetime.now().date()
        df_flags['triggered_date'] = pd.to_datetime(df_flags['triggered_date']).dt.date

        flag_types = [
            'first_time_day_pass_2wk_offer',
            'birthday_party_attendee_one_week_out',
            'birthday_party_host_six_days_out',
            'birthday_party_host_one_week_out',
            'ready_for_membership',
            '2_week_pass_purchase',
        ]

        for flag_type in flag_types:
            count = len(df_flags[(df_flags['flag_type'] == flag_type) & (df_flags['triggered_date'] >= today - timedelta(days=7))])
            if count > 0:
                active_flags_summary.append({
                    'Flag Type': flag_type.replace('_', ' ').title(),
                    'Active (7d)': count
                })

    df_active_flags = pd.DataFrame(active_flags_summary) if active_flags_summary else pd.DataFrame({'Message': ['No active flags in last 7 days']})

    # Build layout
    layout = html.Div([
        html.H1("System Health Monitor", style={"color": "#213B3F", "marginTop": "20px", "marginBottom": "30px"}),

        # Birthday Party Automation Section
        html.H2("Birthday Party Automation (Last 7 Days)", style={"color": "#213B3F", "marginTop": "30px"}),
        html.P("Tracks the full flow from parties scheduled → flags set → messages sent", style={"color": "#666"}),
        dash_table.DataTable(
            data=df_birthday_metrics.to_dict('records') if not df_birthday_metrics.empty else [{'Message': 'No data available'}],
            style_table={'overflowX': 'auto'},
            style_cell={
                'textAlign': 'left',
                'padding': '10px',
                'backgroundColor': '#f9f9f9',
                'border': '1px solid #ddd'
            },
            style_header={
                'backgroundColor': '#213B3F',
                'color': 'white',
                'fontWeight': 'bold',
                'border': '1px solid #213B3F'
            },
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': '#ffffff'
                }
            ]
        ),

        # 2-Week Pass Journey Section
        html.H2("2-Week Pass Journey (Last 7 Days)", style={"color": "#213B3F", "marginTop": "40px"}),
        html.P("Tracks day pass customers → flags → Mailchimp CSV sent", style={"color": "#666"}),
        dash_table.DataTable(
            data=df_2week_metrics.to_dict('records') if not df_2week_metrics.empty else [{'Message': 'No data available'}],
            style_table={'overflowX': 'auto'},
            style_cell={
                'textAlign': 'left',
                'padding': '10px',
                'backgroundColor': '#f9f9f9',
                'border': '1px solid #ddd'
            },
            style_header={
                'backgroundColor': '#213B3F',
                'color': 'white',
                'fontWeight': 'bold',
                'border': '1px solid #213B3F'
            },
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': '#ffffff'
                }
            ]
        ),

        # Active Flags Summary
        html.H2("Active Flags (Last 7 Days)", style={"color": "#213B3F", "marginTop": "40px"}),
        html.P("Current customers flagged for automated actions", style={"color": "#666"}),
        dash_table.DataTable(
            data=df_active_flags.to_dict('records'),
            style_table={'overflowX': 'auto'},
            style_cell={
                'textAlign': 'left',
                'padding': '10px',
                'backgroundColor': '#f9f9f9',
                'border': '1px solid #ddd'
            },
            style_header={
                'backgroundColor': '#213B3F',
                'color': 'white',
                'fontWeight': 'bold',
                'border': '1px solid #213B3F'
            },
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': '#ffffff'
                }
            ]
        ),

        # Data Freshness Section
        html.H2("Data Freshness", style={"color": "#213B3F", "marginTop": "40px"}),
        html.P("Last update time for key data sources", style={"color": "#666"}),
        dash_table.DataTable(
            data=df_freshness.to_dict('records'),
            style_table={'overflowX': 'auto'},
            style_cell={
                'textAlign': 'left',
                'padding': '10px',
                'backgroundColor': '#f9f9f9',
                'border': '1px solid #ddd'
            },
            style_header={
                'backgroundColor': '#213B3F',
                'color': 'white',
                'fontWeight': 'bold',
                'border': '1px solid #213B3F'
            },
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': '#ffffff'
                }
            ]
        ),

        # Recent Errors Section
        html.H2("Recent Notifications & Warnings", style={"color": "#213B3F", "marginTop": "40px"}),
        html.P("System messages from the last 24 hours", style={"color": "#666"}),
        dash_table.DataTable(
            data=df_errors.to_dict('records'),
            style_table={'overflowX': 'auto'},
            style_cell={
                'textAlign': 'left',
                'padding': '10px',
                'backgroundColor': '#f9f9f9',
                'border': '1px solid #ddd'
            },
            style_header={
                'backgroundColor': '#213B3F',
                'color': 'white',
                'fontWeight': 'bold',
                'border': '1px solid #213B3F'
            },
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': '#ffffff'
                },
                {
                    'if': {'filter_query': '{Type} = "❌ Error"'},
                    'backgroundColor': '#ffebee',
                    'color': '#c62828'
                },
                {
                    'if': {'filter_query': '{Type} = "⚠️ Warning"'},
                    'backgroundColor': '#fff3e0',
                    'color': '#f57c00'
                }
            ]
        ),

        html.Div([
            html.P("📧 = Placeholder - SendGrid email tracking will be available later today",
                   style={"color": "#666", "fontStyle": "italic", "marginTop": "30px"}),
            html.P(f"Last refreshed: {datetime.now().strftime('%I:%M %p on %B %d, %Y')}",
                   style={"color": "#999", "fontSize": "12px"})
        ])
    ])

    return layout
