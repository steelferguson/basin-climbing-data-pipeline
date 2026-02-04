import sys

sys.path.append("./src")
from dash import html, dcc, dash_table, Input, Output
import plotly.express as px
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objects as go
from data_pipeline import upload_data
from data_pipeline import config
import os
import plotly.io as pio
from data_pipeline import pipeline_handler
from dashboard import system_health

pio.templates.default = "plotly"  # start off with plotly template as a clean slate


def load_df_from_s3(bucket, key):
    uploader = upload_data.DataUploader()
    csv_content = uploader.download_from_s3(bucket, key)
    return uploader.convert_csv_to_df(csv_content)


def load_data():
    # Load all needed DataFrames from S3
    df_transactions = load_df_from_s3(config.aws_bucket_name, config.s3_path_combined)
    df_memberships = load_df_from_s3(
        config.aws_bucket_name, config.s3_path_capitan_memberships
    )
    df_members = load_df_from_s3(config.aws_bucket_name, config.s3_path_capitan_members)
    df_projection = load_df_from_s3(
        config.aws_bucket_name, config.s3_path_capitan_membership_revenue_projection
    )
    df_at_risk = load_df_from_s3(config.aws_bucket_name, config.s3_path_at_risk_members)
    df_facebook_ads = load_df_from_s3(config.aws_bucket_name, config.s3_path_facebook_ads)
    df_events = load_df_from_s3(config.aws_bucket_name, config.s3_path_capitan_events)
    df_customer_events = load_df_from_s3(config.aws_bucket_name, config.s3_path_customer_events)

    return df_memberships, df_members, df_transactions, df_projection, df_at_risk, df_facebook_ads, df_events, df_customer_events


def create_dashboard(app):

    df_memberships, df_members, df_combined, df_projection, df_at_risk, df_facebook_ads, df_events, df_customer_events = load_data()

    # Prepare at-risk members data for display
    if not df_at_risk.empty:
        # Create full name column
        df_at_risk['full_name'] = df_at_risk['first_name'] + ' ' + df_at_risk['last_name']
        # Format customer_id as markdown link
        df_at_risk['customer_id_link'] = df_at_risk.apply(
            lambda row: f"[{row['customer_id']}]({row['capitan_link']})" if 'capitan_link' in row and pd.notna(row['capitan_link']) else str(row['customer_id']),
            axis=1
        )
        # Format last_checkin_date
        df_at_risk['last_checkin_formatted'] = pd.to_datetime(df_at_risk['last_checkin_date'], errors='coerce').dt.strftime('%Y-%m-%d')
        df_at_risk['last_checkin_formatted'] = df_at_risk['last_checkin_formatted'].fillna('Never')
        # Format age
        df_at_risk['age_formatted'] = df_at_risk['age'].apply(lambda x: str(int(x)) if pd.notna(x) else 'N/A')
        # Format membership_type
        df_at_risk['membership_type'] = df_at_risk['membership_type'].fillna('Unknown')
        # Get timestamp for display
        at_risk_timestamp = df_at_risk['generated_at'].iloc[0] if 'generated_at' in df_at_risk.columns else 'N/A'
    else:
        at_risk_timestamp = 'N/A'

    app.layout = html.Div([
        dcc.Tabs(id='tabs', value='business-metrics', children=[
            dcc.Tab(label='Business Metrics', value='business-metrics', children=[
                html.Div([
            # Timeframe toggle
            dcc.RadioItems(
                id="timeframe-toggle",
                options=[
                    {"label": "Day", "value": "D"},
                    {"label": "Week", "value": "W"},
                    {"label": "Month", "value": "M"},
                ],
                value="M",  # Default to "Month"
                inline=True,
                style={
                    "backgroundColor": "#213B3F",
                    "padding": "10px",
                    "borderRadius": "5px",
                    "color": "#FFFFFF",
                    "marginBottom": "20px",
                },
            ),
            # Data source toggle for all revenue charts
            dcc.Checklist(
                id="source-toggle",
                options=[
                    {"label": "Square", "value": "Square"},
                    {"label": "Stripe", "value": "Stripe"},
                ],
                value=["Square", "Stripe"],
                inline=True,
                style={"marginBottom": "20px"},
            ),
            # Total Revenue chart section
            html.H1(
                children="Total Revenue Over Time",
                style={"color": "#213B3F", "marginTop": "30px"},
            ),
            dcc.Graph(id="total-revenue-chart"),
            # Square and Stripe Revenue section
            html.Div(
                [
                    html.H1(
                        children="Square and Stripe Revenue Analysis",
                        style={"color": "#213B3F", "marginTop": "30px"},
                    ),
                    dcc.Graph(id="square-stripe-revenue-chart"),
                    dcc.Graph(id="square-stripe-revenue-stacked-chart"),
                    dcc.Graph(id="revenue-percentage-chart"),
                    dcc.Graph(id="refund-rate-chart"),
                    dcc.Graph(id="revenue-accounting-groups-chart"),
                ],
                style={"marginBottom": "40px"},
            ),
            # Day Pass Count chart section
            html.H1(
                children="Day Pass Count",
                style={"color": "#213B3F", "marginTop": "30px"},
            ),
            dcc.Graph(id="day-pass-count-chart"),
            # Membership Revenue Projection chart section
            html.H1(
                children="Membership Revenue Projections (Current Month + 3 Months)",
                style={"color": "#213B3F", "marginTop": "30px"},
            ),
            html.Div(
                [
                    html.H3("Membership Frequency:"),
                    dcc.Checklist(
                        id="projection-frequency-toggle",
                        options=[
                            {"label": "Annual", "value": "yearly"},
                            {"label": "Monthly", "value": "monthly"},
                            {"label": "Bi-Weekly", "value": "bi_weekly"},
                            {"label": "Prepaid", "value": "prepaid"},
                        ],
                        value=["yearly", "monthly", "bi_weekly", "prepaid"],
                        inline=True,
                        style={"margin-bottom": "20px"},
                    ),
                    html.H3("Show Total Line:"),
                    dcc.Checklist(
                        id="show-total-toggle",
                        options=[{"label": "Show Total", "value": "show_total"}],
                        value=["show_total"],
                        inline=True,
                    ),
                ]
            ),
            dcc.Graph(id="membership-revenue-projection-chart"),
            # Membership Timeline chart section
            html.H1(
                children="Membership Timeline",
                style={"color": "#213B3F", "marginTop": "30px"},
            ),
            html.Div(
                [
                    html.H3("Membership Status:"),
                    dcc.Checklist(
                        id="status-toggle",
                        options=[
                            {"label": "Active", "value": "ACT"},
                            {"label": "Ended", "value": "END"},
                            {"label": "Frozen", "value": "FRZ"},
                        ],
                        value=["ACT", "END"],  # Default to active and ended
                        inline=True,
                        style={"margin-bottom": "20px"},
                    ),
                    html.H3("Membership Frequency:"),
                    dcc.Checklist(
                        id="frequency-toggle",
                        options=[
                            {"label": "Bi-Weekly", "value": "bi_weekly"},
                            {"label": "Monthly", "value": "monthly"},
                            {"label": "Annual", "value": "annual"},
                            {"label": "3 Month Prepaid", "value": "prepaid_3mo"},
                            {"label": "6 Month Prepaid", "value": "prepaid_6mo"},
                            {"label": "12 Month Prepaid", "value": "prepaid_12mo"},
                        ],
                        value=[
                            "bi_weekly",
                            "monthly",
                            "annual",
                            "prepaid_3mo",
                            "prepaid_6mo",
                            "prepaid_12mo",
                        ],
                        inline=True,
                        style={"margin-bottom": "20px"},
                    ),
                    html.H3("Membership Size:"),
                    dcc.Checklist(
                        id="size-toggle",
                        options=[
                            {"label": "Solo", "value": "solo"},
                            {"label": "Duo", "value": "duo"},
                            {"label": "Family", "value": "family"},
                            {"label": "Corporate", "value": "corporate"},
                        ],
                        value=["solo", "duo", "family", "corporate"],
                        inline=True,
                        style={"margin-bottom": "20px"},
                    ),
                    html.H3("Special Categories:"),
                    dcc.Checklist(
                        id="category-toggle",
                        options=[
                            {"label": "Founder", "value": "founder"},
                            {"label": "College", "value": "college"},
                            {"label": "Corporate", "value": "corporate"},
                            {"label": "Mid-Day", "value": "mid_day"},
                            {"label": "Fitness Only", "value": "fitness_only"},
                            {
                                "label": "Has Fitness Addon",
                                "value": "has_fitness_addon",
                            },
                            {"label": "Team Dues", "value": "team_dues"},
                            {"label": "90 for 90", "value": "90_for_90"},
                            {"label": "Include BCF Staff", "value": "include_bcf"},
                            {
                                "label": "Not in a Special Category",
                                "value": "not_special",
                            },
                        ],
                        value=[
                            "founder",
                            "college",
                            "corporate",
                            "mid_day",
                            "fitness_only",
                            "has_fitness_addon",
                            "team_dues",
                            "90_for_90",
                            "not_special",
                        ],
                        inline=True,
                        style={"margin-bottom": "20px"},
                    ),
                ]
            ),
            dcc.Graph(id="membership-timeline-chart"),
            # Members over time chart section
            dcc.Graph(id="members-timeline-chart"),
            # Membership Attrition and New Membership chart section
            html.H1(
                children="Membership Attrition & New Membership",
                style={"color": "#213B3F", "marginTop": "30px"},
            ),
            dcc.Graph(id="membership-attrition-new-chart"),
            # New vs Existing Memberships chart section
            html.H1(
                children="Active Memberships: New vs Existing",
                style={"color": "#213B3F", "marginTop": "30px"},
            ),
            dcc.Graph(id="membership-new-vs-existing-chart"),
            # Youth Teams section
            html.H1(
                children="Youth Teams Membership",
                style={"color": "#213B3F", "marginTop": "30px"},
            ),
            dcc.Graph(id="youth-teams-chart"),
            # Birthday Rentals section
            html.H1(
                children="Birthday Rentals",
                style={"color": "#213B3F", "marginTop": "30px"},
            ),
            html.Div(
                [
                    dcc.Graph(id="birthday-participants-chart"),
                    dcc.Graph(id="birthday-revenue-chart"),
                ],
                style={
                    "backgroundColor": "#F5F5F5",
                    "padding": "20px",
                    "borderRadius": "10px",
                    "marginBottom": "40px",
                },
            ),
            # Fitness Revenue section
            html.H1(
                children="Fitness Revenue & Class Attendance",
                style={"color": "#213B3F", "marginTop": "30px"},
            ),
            html.Div(
                [
                    dcc.Graph(id="fitness-revenue-chart"),
                    dcc.Graph(id="fitness-class-attendance-chart"),
                ],
                style={
                    "backgroundColor": "#F5F5F5",
                    "padding": "20px",
                    "borderRadius": "10px",
                    "marginBottom": "40px",
                },
            ),
            # Marketing Performance section
            html.H1(
                children="Marketing Performance (Last 3 Months)",
                style={"color": "#213B3F", "marginTop": "30px"},
            ),
            dcc.Graph(id="marketing-performance-chart"),
            # Camps Revenue section
            html.H1(
                children="Camps Revenue",
                style={"color": "#213B3F", "marginTop": "30px"},
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.H2(
                                "Camp Session Purchases",
                                style={"textAlign": "center", "marginBottom": "20px"},
                            ),
                            dcc.Graph(id="camp-sessions-chart"),
                        ],
                        style={"marginBottom": "40px"},
                    ),
                    html.Div(
                        [
                            html.H2(
                                "Camp Revenue",
                                style={"textAlign": "center", "marginBottom": "20px"},
                            ),
                            dcc.Graph(id="camp-revenue-chart"),
                        ],
                        style={"marginBottom": "40px"},
                    ),
                ],
                style={
                    "backgroundColor": "#F5F5F5",
                    "padding": "20px",
                    "borderRadius": "10px",
                    "marginBottom": "40px",
                },
            ),
            # 90 for 90 Membership Analysis section
            html.H1(
                children="90 for 90 Membership Analysis",
                style={"color": "#213B3F", "marginTop": "30px"},
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.H2(
                                "90 for 90 Purchase Volume by Week",
                                style={"textAlign": "center", "marginBottom": "20px"},
                            ),
                            dcc.Graph(id="ninety-for-ninety-timeline-chart"),
                        ],
                        style={"marginBottom": "40px"},
                    ),
                    html.Div(
                        [
                            html.H2(
                                "90 for 90 Conversion Summary",
                                style={"textAlign": "center", "marginBottom": "20px"},
                            ),
                            dcc.Graph(id="ninety-for-ninety-summary-chart"),
                        ],
                        style={"marginBottom": "40px"},
                    ),
                ],
                style={
                    "backgroundColor": "#F5F5F5",
                    "padding": "20px",
                    "borderRadius": "10px",
                    "marginBottom": "40px",
                },
            ),
            # At-Risk Members section
            html.H1(
                children="At-Risk Members",
                style={"color": "#213B3F", "marginTop": "60px"},
            ),
            html.H3(
                children=f"As of {at_risk_timestamp}",
                style={"color": "#26241C", "marginBottom": "30px"},
            ),
            # Declining Activity category
            html.Div(
                [
                    html.H2(
                        children=f"Declining Activity ({len(df_at_risk[df_at_risk['risk_category'] == 'Declining Activity'])} members)" if not df_at_risk.empty else "Declining Activity (0 members)",
                        style={"color": "#213B3F", "marginBottom": "10px"},
                    ),
                    dash_table.DataTable(
                        data=df_at_risk[df_at_risk['risk_category'] == 'Declining Activity'][['customer_id_link', 'full_name', 'age_formatted', 'membership_type', 'last_checkin_formatted', 'risk_description']].to_dict('records') if not df_at_risk.empty and len(df_at_risk[df_at_risk['risk_category'] == 'Declining Activity']) > 0 else [],
                        columns=[
                            {"name": "Customer ID", "id": "customer_id_link", "presentation": "markdown"},
                            {"name": "Name", "id": "full_name"},
                            {"name": "Age", "id": "age_formatted"},
                            {"name": "Membership Type", "id": "membership_type"},
                            {"name": "Last Check-in", "id": "last_checkin_formatted"},
                            {"name": "Description", "id": "risk_description"},
                        ],
                        style_table={'overflowX': 'auto'},
                        style_cell={
                            'textAlign': 'left',
                            'padding': '10px',
                            'whiteSpace': 'normal',
                            'height': 'auto',
                        },
                        style_header={
                            'backgroundColor': '#213B3F',
                            'color': 'white',
                            'fontWeight': 'bold',
                        },
                        style_data={
                            'backgroundColor': '#FFFFFF',
                            'color': '#26241C',
                        },
                        markdown_options={"link_target": "_blank"},
                    ) if not df_at_risk.empty and len(df_at_risk[df_at_risk['risk_category'] == 'Declining Activity']) > 0 else html.P("No members in this category", style={"fontStyle": "italic", "color": "#808080"}),
                ],
                style={"marginBottom": "40px"},
            ),
            # Completely Inactive category
            html.Div(
                [
                    html.H2(
                        children=f"Completely Inactive ({len(df_at_risk[df_at_risk['risk_category'] == 'Completely Inactive'])} members)" if not df_at_risk.empty else "Completely Inactive (0 members)",
                        style={"color": "#213B3F", "marginBottom": "10px"},
                    ),
                    dash_table.DataTable(
                        data=df_at_risk[df_at_risk['risk_category'] == 'Completely Inactive'][['customer_id_link', 'full_name', 'age_formatted', 'membership_type', 'last_checkin_formatted', 'risk_description']].to_dict('records') if not df_at_risk.empty and len(df_at_risk[df_at_risk['risk_category'] == 'Completely Inactive']) > 0 else [],
                        columns=[
                            {"name": "Customer ID", "id": "customer_id_link", "presentation": "markdown"},
                            {"name": "Name", "id": "full_name"},
                            {"name": "Age", "id": "age_formatted"},
                            {"name": "Membership Type", "id": "membership_type"},
                            {"name": "Last Check-in", "id": "last_checkin_formatted"},
                            {"name": "Description", "id": "risk_description"},
                        ],
                        style_table={'overflowX': 'auto'},
                        style_cell={
                            'textAlign': 'left',
                            'padding': '10px',
                            'whiteSpace': 'normal',
                            'height': 'auto',
                        },
                        style_header={
                            'backgroundColor': '#213B3F',
                            'color': 'white',
                            'fontWeight': 'bold',
                        },
                        style_data={
                            'backgroundColor': '#FFFFFF',
                            'color': '#26241C',
                        },
                        markdown_options={"link_target": "_blank"},
                    ) if not df_at_risk.empty and len(df_at_risk[df_at_risk['risk_category'] == 'Completely Inactive']) > 0 else html.P("No members in this category", style={"fontStyle": "italic", "color": "#808080"}),
                ],
                style={"marginBottom": "40px"},
            ),
        ],
        style={
            "margin": "0 auto",
            "maxWidth": "1200px",
            "padding": "20px",
            "backgroundColor": "#FFFFFF",
            "color": "#26241C",
            "fontFamily": "Arial, sans-serif",
        })  # Close html.Div for Business Metrics tab
            ]),  # Close dcc.Tab for Business Metrics

            # System Health Tab
            dcc.Tab(label='System Health', value='system-health', children=[
                html.Div([
                    system_health.create_system_health_layout()
                ],
                style={
                    "margin": "0 auto",
                    "maxWidth": "1200px",
                    "padding": "20px",
                    "backgroundColor": "#FFFFFF",
                    "color": "#26241C",
                    "fontFamily": "Arial, sans-serif",
                })  # Close html.Div for System Health tab
            ]),  # Close dcc.Tab for System Health

        ])  # Close dcc.Tabs
    ])  # Close outer html.Div (app.layout)

    # Update the color scheme for all charts
    chart_colors = {
        "primary": "#AF5436",  # rust
        "secondary": "#E9C867",  # gold
        "tertiary": "#BCCDA3",  # sage
        "quaternary": "#213B3F",  # dark teal
        "background": "#F5F5F5",  # light grey background
        "text": "#26241C",  # dark grey
    }

    # Define a sequence of colors for categorical data
    categorical_colors = [
        chart_colors["primary"],  # rust
        chart_colors["secondary"],  # gold
        chart_colors["tertiary"],  # sage
        chart_colors["quaternary"],  # dark teal
        "#8B4229",  # darker rust
        "#BAA052",  # darker gold
        "#96A682",  # darker sage
        "#1A2E31",  # darker teal
    ]

    # Callback for Total Revenue chart
    @app.callback(
        Output("total-revenue-chart", "figure"),
        [Input("timeframe-toggle", "value"), Input("source-toggle", "value")],
    )
    def update_total_revenue_chart(selected_timeframe, selected_sources):
        df_filtered = df_combined[
            df_combined["Data Source"].isin(selected_sources)
        ].copy()
        df_filtered["Date"] = pd.to_datetime(df_filtered["Date"], errors="coerce")
        df_filtered["Date"] = df_filtered["Date"].dt.tz_localize(None)
        df_filtered["date"] = (
            df_filtered["Date"].dt.to_period(selected_timeframe).dt.start_time
        )

        total_revenue = df_filtered.groupby("date")["Total Amount"].sum().reset_index()

        fig = px.line(
            total_revenue, x="date", y="Total Amount", title="Total Revenue Over Time"
        )
        fig.update_traces(line_color=chart_colors["primary"])
        fig.update_layout(
            plot_bgcolor=chart_colors["background"],
            paper_bgcolor=chart_colors["background"],
            font_color=chart_colors["text"],
        )
        return fig

    # Callback for Square and Stripe charts
    @app.callback(
        [
            Output("square-stripe-revenue-chart", "figure"),
            Output("square-stripe-revenue-stacked-chart", "figure"),
            Output("revenue-percentage-chart", "figure"),
            Output("refund-rate-chart", "figure"),
            Output("revenue-accounting-groups-chart", "figure"),
        ],
        [Input("timeframe-toggle", "value"), Input("source-toggle", "value")],
    )
    def update_square_stripe_charts(selected_timeframe, selected_sources):
        # Define revenue category colors and order
        revenue_category_colors = {
            "New Membership": chart_colors["secondary"],  # Gold
            "Membership Renewal": chart_colors["quaternary"],  # Teal
            "Day Pass": chart_colors["primary"],  # Rust
            "Other": chart_colors["tertiary"],  # Sage
        }

        # Define the order of categories
        category_order = ["New Membership", "Membership Renewal", "Day Pass", "Other"]

        # Filter and resample the Square and Stripe data
        df_filtered = df_combined[df_combined["Data Source"].isin(selected_sources)]
        df_filtered["Date"] = pd.to_datetime(df_filtered["Date"], errors="coerce")
        df_filtered["Date"] = df_filtered["Date"].dt.tz_localize(None)
        df_filtered["date"] = (
            df_filtered["Date"].dt.to_period(selected_timeframe).dt.start_time
        )
        revenue_by_category = (
            df_filtered.groupby(["date", "revenue_category"])["Total Amount"]
            .sum()
            .reset_index()
        )

        # Line chart
        line_fig = px.line(
            revenue_by_category,
            x="date",
            y="Total Amount",
            color="revenue_category",
            title="Revenue By Category Over Time",
            category_orders={"revenue_category": category_order},
        )
        line_fig.update_layout(
            plot_bgcolor=chart_colors["background"],
            paper_bgcolor=chart_colors["background"],
            font_color=chart_colors["text"],
        )
        for category in revenue_category_colors:
            line_fig.update_traces(
                line_color=revenue_category_colors[category],
                selector=dict(name=category),
            )

        # Stacked column chart
        stacked_fig = px.bar(
            revenue_by_category,
            x="date",
            y="Total Amount",
            color="revenue_category",
            title="Revenue (Stacked Column)",
            barmode="stack",
            category_orders={"revenue_category": category_order},
        )
        stacked_fig.update_layout(
            plot_bgcolor=chart_colors["background"],
            paper_bgcolor=chart_colors["background"],
            font_color=chart_colors["text"],
        )
        for category in revenue_category_colors:
            stacked_fig.update_traces(
                marker_color=revenue_category_colors[category],
                selector=dict(name=category),
            )

        # Percentage chart
        total_revenue_per_date = (
            revenue_by_category.groupby("date")["Total Amount"].sum().reset_index()
        )
        total_revenue_per_date.columns = ["date", "total_revenue"]
        revenue_with_total = pd.merge(
            revenue_by_category, total_revenue_per_date, on="date"
        )
        revenue_with_total["percentage"] = (
            revenue_with_total["Total Amount"] / revenue_with_total["total_revenue"]
        ) * 100

        percentage_fig = px.bar(
            revenue_with_total,
            x="date",
            y="percentage",
            color="revenue_category",
            title="Percentage of Revenue by Category",
            barmode="stack",
            category_orders={"revenue_category": category_order},
            text=revenue_with_total["percentage"].apply(lambda x: f"{x:.1f}%"),
        )
        percentage_fig.update_traces(
            textposition="inside",
            textfont=dict(size=12, color="white"),
        )
        percentage_fig.update_layout(
            plot_bgcolor=chart_colors["background"],
            paper_bgcolor=chart_colors["background"],
            font_color=chart_colors["text"],
        )
        for category in revenue_category_colors:
            percentage_fig.update_traces(
                marker_color=revenue_category_colors[category],
                selector=dict(name=category),
            )

        # Refund rate chart
        # Calculate gross revenue (positive amounts) and refunds (negative amounts) by category
        df_filtered_copy = df_filtered.copy()
        df_filtered_copy['is_refund'] = df_filtered_copy['Total Amount'] < 0

        refund_stats = df_filtered_copy.groupby('revenue_category').agg({
            'Total Amount': lambda x: {
                'gross': x[x > 0].sum(),
                'refunds': abs(x[x < 0].sum()),
                'net': x.sum()
            }
        }).reset_index()

        # Expand the dict into columns
        refund_stats['gross_revenue'] = refund_stats['Total Amount'].apply(lambda x: x['gross'])
        refund_stats['refunds'] = refund_stats['Total Amount'].apply(lambda x: x['refunds'])
        refund_stats['net_revenue'] = refund_stats['Total Amount'].apply(lambda x: x['net'])
        refund_stats.drop('Total Amount', axis=1, inplace=True)

        # Calculate refund rate percentage
        refund_stats['refund_rate'] = (refund_stats['refunds'] / refund_stats['gross_revenue'] * 100).fillna(0)

        # Filter out categories with no gross revenue
        refund_stats = refund_stats[refund_stats['gross_revenue'] > 0]

        # Sort by refund rate descending
        refund_stats = refund_stats.sort_values('refund_rate', ascending=False)

        # Create horizontal bar chart for refund rates
        refund_fig = px.bar(
            refund_stats,
            y='revenue_category',
            x='refund_rate',
            title='Refund Rate by Category (%)',
            orientation='h',
            text='refund_rate',
            labels={'refund_rate': 'Refund Rate (%)', 'revenue_category': 'Category'}
        )

        # Update text to show percentage with 1 decimal
        refund_fig.update_traces(
            texttemplate='%{text:.1f}%',
            textposition='outside',
            marker_color=chart_colors["primary"]
        )

        refund_fig.update_layout(
            plot_bgcolor=chart_colors["background"],
            paper_bgcolor=chart_colors["background"],
            font_color=chart_colors["text"],
            xaxis_title="Refund Rate (%)",
            yaxis_title="Category",
            height=400
        )

        # Accounting groups chart (combine categories for accounting purposes)
        # Group: Memberships (New + Renewing), Team & Programming (Team Dues + Programming)
        accounting_groups = revenue_by_category.copy()

        # Create accounting group mapping
        def map_to_accounting_group(category):
            if category in ['New Membership', 'Membership Renewal']:
                return 'Memberships'
            elif category in ['Team Dues', 'Programming']:
                return 'Team & Programming'
            else:
                return category

        accounting_groups['accounting_group'] = accounting_groups['revenue_category'].apply(map_to_accounting_group)

        # Aggregate by accounting group
        accounting_revenue = accounting_groups.groupby(['date', 'accounting_group'])['Total Amount'].sum().reset_index()

        # Calculate percentages
        accounting_total = accounting_revenue.groupby('date')['Total Amount'].sum().reset_index()
        accounting_total.columns = ['date', 'total_revenue']
        accounting_with_total = pd.merge(accounting_revenue, accounting_total, on='date')
        accounting_with_total['percentage'] = (accounting_with_total['Total Amount'] / accounting_with_total['total_revenue']) * 100

        # Define color mapping for accounting groups (reuse existing colors where possible)
        accounting_colors = {
            'Memberships': revenue_category_colors.get('Membership Renewal', chart_colors["primary"]),
            'Team & Programming': revenue_category_colors.get('Programming', chart_colors["secondary"]),
            'Day Pass': revenue_category_colors.get('Day Pass', chart_colors["quaternary"]),
            'Retail': revenue_category_colors.get('Retail', chart_colors["tertiary"]),
            'Event Booking': revenue_category_colors.get('Event Booking', '#8B4229'),
        }

        accounting_fig = px.bar(
            accounting_with_total,
            x='date',
            y='percentage',
            color='accounting_group',
            title='Revenue by Accounting Groups (Memberships, Team & Programming, etc.)',
            barmode='stack',
            text=accounting_with_total['percentage'].apply(lambda x: f'{x:.1f}%'),
        )

        accounting_fig.update_traces(
            textposition='inside',
            textfont=dict(size=12, color='white'),
        )

        accounting_fig.update_layout(
            plot_bgcolor=chart_colors["background"],
            paper_bgcolor=chart_colors["background"],
            font_color=chart_colors["text"],
            yaxis_title='Percentage (%)',
            xaxis_title='Date',
        )

        # Apply colors
        for group in accounting_colors:
            accounting_fig.update_traces(
                marker_color=accounting_colors[group],
                selector=dict(name=group),
            )

        return line_fig, stacked_fig, percentage_fig, refund_fig, accounting_fig

    # Callback for Day Pass chart
    @app.callback(
        Output("day-pass-count-chart", "figure"), [Input("timeframe-toggle", "value")]
    )
    def update_day_pass_chart(selected_timeframe):
        # Get day pass purchases from customer_events
        df_day_passes = df_customer_events[
            df_customer_events["event_type"] == "day_pass_purchase"
        ].copy()

        if df_day_passes.empty:
            # Return empty chart if no data
            fig = px.bar(title="Day Pass Purchases by Customer Type")
            fig.update_layout(
                plot_bgcolor=chart_colors["background"],
                paper_bgcolor=chart_colors["background"],
                font_color=chart_colors["text"],
            )
            return fig

        df_day_passes["event_date"] = pd.to_datetime(df_day_passes["event_date"], errors="coerce")

        # For each day pass purchase, determine customer type based on prior activity
        customer_types = []
        for _, purchase in df_day_passes.iterrows():
            customer_id = purchase["customer_id"]
            purchase_date = purchase["event_date"]

            # Get all prior events for this customer (excluding flag_set events)
            prior_events = df_customer_events[
                (df_customer_events["customer_id"] == customer_id) &
                (df_customer_events["event_date"] < purchase_date) &
                (df_customer_events["event_type"] != "flag_set")
            ]

            if len(prior_events) == 0:
                customer_type = "New Customer"
            else:
                # Get most recent prior event date
                last_activity = prior_events["event_date"].max()
                days_since = (purchase_date - pd.to_datetime(last_activity)).days

                if days_since <= 60:  # 0-2 months
                    customer_type = "Returning (0-2mo)"
                elif days_since <= 180:  # 2-6 months
                    customer_type = "Returning (2-6mo)"
                else:  # 6+ months
                    customer_type = "Returning (6+mo)"

            customer_types.append(customer_type)

        df_day_passes["customer_type"] = customer_types

        # Group by time period and customer type
        df_day_passes["date"] = (
            df_day_passes["event_date"].dt.to_period(selected_timeframe).dt.start_time
        )

        day_pass_grouped = (
            df_day_passes.groupby(["date", "customer_type"])
            .size()
            .reset_index(name="count")
        )

        # Define category order and colors
        category_order = ["New Customer", "Returning (0-2mo)", "Returning (2-6mo)", "Returning (6+mo)"]
        category_colors = {
            "New Customer": chart_colors["primary"],  # Rust
            "Returning (0-2mo)": chart_colors["secondary"],  # Gold
            "Returning (2-6mo)": chart_colors["quaternary"],  # Teal
            "Returning (6+mo)": chart_colors["tertiary"],  # Sage
        }

        fig = px.bar(
            day_pass_grouped,
            x="date",
            y="count",
            color="customer_type",
            title="Day Pass Purchases by Customer Type",
            category_orders={"customer_type": category_order},
            color_discrete_map=category_colors,
            labels={"count": "Day Passes", "date": "Date", "customer_type": "Customer Type"}
        )

        fig.update_layout(
            plot_bgcolor=chart_colors["background"],
            paper_bgcolor=chart_colors["background"],
            font_color=chart_colors["text"],
            barmode="stack",
            legend=dict(
                title="Customer Type",
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.02
            )
        )
        return fig

    # Callback to update the Membership Revenue Projection chart
    @app.callback(
        Output("membership-revenue-projection-chart", "figure"),
        [
            Input("timeframe-toggle", "value"),
            Input("projection-frequency-toggle", "value"),
            Input("show-total-toggle", "value"),
        ],
    )
    def update_membership_revenue_projection_chart(
        selected_timeframe, selected_frequencies, show_total
    ):
        # Filter for membership-related revenue categories
        membership_cats = ["Membership Renewal", "New Membership"]
        df_historical = df_combined[
            df_combined["revenue_category"].isin(membership_cats)
        ].copy()

        # Convert Date to period and group
        df_historical["Date"] = pd.to_datetime(df_historical["Date"], errors="coerce")
        # filter to only the past 3 montths plus this current month
        df_historical = df_historical[
            df_historical["Date"] >= (pd.Timestamp.now() - pd.DateOffset(months=3))
        ]
        df_historical["period"] = (
            df_historical["Date"].dt.to_period(selected_timeframe).dt.start_time
        )

        # Group by period and (optionally) membership_size or other columns
        historical_revenue_by_period = (
            df_historical.groupby("period")["Total Amount"].sum().reset_index()
        )
        # rename Total Amount to historical_total
        historical_revenue_by_period.rename(
            columns={"Total Amount": "historical_total"}, inplace=True
        )

        df_proj = df_projection.copy()
        df_proj["date"] = pd.to_datetime(df_proj["date"], errors="coerce")
        df_proj["date"] = df_proj["date"].dt.tz_localize(None)
        df_proj["period"] = (
            df_proj["date"].dt.to_period(selected_timeframe).dt.start_time
        )

        # Group by period and (optionally) membership_size or other columns
        projection_revenue_by_period = (
            df_proj.groupby("period")["projected_total"].sum().reset_index()
        )

        # Combine historical and projection data
        revenue_by_period = pd.concat(
            [historical_revenue_by_period, projection_revenue_by_period]
        )

        # Plot stacked bar chart
        fig = px.bar(
            revenue_by_period,
            x="period",
            y=["historical_total", "projected_total"],
            title="Membership Revenue Projection",
            barmode="stack",
            color_discrete_map={
                # make historical grey
                "historical_total": "#808080",
                "projected_total": chart_colors["primary"],
            },
        )

        if show_total:
            revenue_by_period["total"] = revenue_by_period["historical_total"].fillna(
                0
            ) + revenue_by_period["projected_total"].fillna(0)
            totals = revenue_by_period.groupby("period")["total"].sum().reset_index()
            fig.add_trace(
                go.Scatter(
                    x=totals["period"],
                    y=totals["total"],
                    mode="text",
                    text=totals["total"].round(0).astype(str),
                    textposition="top center",
                    textfont=dict(size=12, color="black"),
                )
            )

        return fig

    # Callback to update the Membership Timeline chart
    @app.callback(
        Output("membership-timeline-chart", "figure"),
        [
            Input("frequency-toggle", "value"),
            Input("size-toggle", "value"),
            Input("category-toggle", "value"),
            Input("status-toggle", "value"),
        ],
    )
    def update_membership_timeline_chart(
        frequency_toggle, size_toggle, category_toggle, status_toggle
    ):
        # Load the processed membership DataFrame
        df = df_memberships

        # Filter by status
        df = df[df["status"].isin(status_toggle)]
        df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")
        df["end_date"] = df["end_date"].dt.tz_localize(None)
        df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
        df["start_date"] = df["start_date"].dt.tz_localize(None)

        # Filter by frequency and size
        df = df[df["frequency"].isin(frequency_toggle)]
        df = df[df["size"].isin(size_toggle)]

        # Filter by category toggles (if you want to keep these)
        if "include_bcf" not in category_toggle:
            df = df[~df["is_bcf"]]
        if "founder" not in category_toggle:
            df = df[~df["is_founder"]]
        if "college" not in category_toggle:
            df = df[~df["is_college"]]
        if "corporate" not in category_toggle:
            df = df[~df["is_corporate"]]
        if "mid_day" not in category_toggle:
            df = df[~df["is_mid_day"]]
        if "fitness_only" not in category_toggle:
            df = df[~df["is_fitness_only"]]
        if "has_fitness_addon" not in category_toggle:
            df = df[~df["has_fitness_addon"]]
        if "team_dues" not in category_toggle:
            df = df[~df["is_team_dues"]]
        if "90_for_90" not in category_toggle:
            df = df[~df["is_90_for_90"]]
        if "not_special" in category_toggle:
            # When "not_special" is selected, show ONLY members NOT in special categories
            df = df[df["is_not_in_special"]]

        # Create a date range from the earliest start date to today
        min_date = df["start_date"].min()
        max_date = pd.Timestamp.now()
        date_range = pd.date_range(start=min_date, end=max_date, freq="D")

        # Calculate active memberships for each day by frequency
        daily_counts = []
        for date in date_range:
            active = df[(df["start_date"] <= date) & (df["end_date"] >= date)]
            counts = active["frequency"].value_counts().to_dict()
            daily_counts.append(
                {
                    "date": date,
                    **{freq: counts.get(freq, 0) for freq in frequency_toggle},
                }
            )

        daily_counts_df = pd.DataFrame(daily_counts)

        # Plot
        fig = go.Figure()
        frequency_colors = {
            "bi_weekly": "#1f77b4",
            "monthly": "#ff7f0e",
            "annual": "#2ca02c",
            "prepaid_3mo": "#8B4229",
            "prepaid_6mo": "#BAA052",
            "prepaid_12mo": "#96A682",
            "unknown": "#1A2E31",
        }
        for freq in frequency_toggle:
            if freq in daily_counts_df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=daily_counts_df["date"],
                        y=daily_counts_df[freq],
                        mode="lines",
                        name=freq.replace("_", " ").title(),
                        stackgroup="one",
                        line=dict(color=frequency_colors.get(freq, None)),
                    )
                )

        # Add total line
        total = daily_counts_df[frequency_toggle].sum(axis=1)
        fig.add_trace(
            go.Scatter(
                x=daily_counts_df["date"],
                y=total,
                mode="lines",
                name="Total",
                line=dict(color="#222222", width=2, dash="dash"),
                hovertemplate="Total: %{y}<extra></extra>",
            )
        )

        fig.update_layout(
            title="Active Memberships Over Time by Payment Frequency",
            showlegend=True,
            height=600,
            xaxis_title="Date",
            yaxis_title="Number of Active Memberships",
            hovermode="x unified",
        )
        return fig

    # Callback for Members Timeline chart
    @app.callback(
        Output("members-timeline-chart", "figure"),
        [
            Input("frequency-toggle", "value"),
            Input("size-toggle", "value"),
            Input("category-toggle", "value"),
            Input("status-toggle", "value"),
        ],
    )
    def update_members_timeline_chart(
        frequency_toggle, size_toggle, category_toggle, status_toggle
    ):
        # Load the processed members DataFrame
        df = df_members

        # Filter by status
        df = df[df["status"].isin(status_toggle)]
        df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")
        df["end_date"] = df["end_date"].dt.tz_localize(None)
        df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
        df["start_date"] = df["start_date"].dt.tz_localize(None)

        # Filter by frequency and size
        df = df[df["frequency"].isin(frequency_toggle)]
        df = df[df["size"].isin(size_toggle)]

        # Filter by category toggles (if you want to keep these)
        if "include_bcf" not in category_toggle:
            df = df[~df["is_bcf"]]
        if "founder" not in category_toggle:
            df = df[~df["is_founder"]]
        if "college" not in category_toggle:
            df = df[~df["is_college"]]
        if "corporate" not in category_toggle:
            df = df[~df["is_corporate"]]
        if "mid_day" not in category_toggle:
            df = df[~df["is_mid_day"]]
        if "fitness_only" not in category_toggle:
            df = df[~df["is_fitness_only"]]
        if "has_fitness_addon" not in category_toggle:
            df = df[~df["has_fitness_addon"]]
        if "team_dues" not in category_toggle:
            df = df[~df["is_team_dues"]]
        if "90_for_90" not in category_toggle:
            df = df[~df["is_90_for_90"]]
        if "not_special" in category_toggle:
            # When "not_special" is selected, show ONLY members NOT in special categories
            df = df[df["is_not_in_special"]]

        # Create a date range from the earliest start date to today
        min_date = df["start_date"].min()
        max_date = pd.Timestamp.now()
        date_range = pd.date_range(start=min_date, end=max_date, freq="D")

        # Calculate active members for each day by frequency
        daily_counts = []
        for date in date_range:
            active = df[(df["start_date"] <= date) & (df["end_date"] >= date)]
            counts = active["frequency"].value_counts().to_dict()
            daily_counts.append(
                {
                    "date": date,
                    **{freq: counts.get(freq, 0) for freq in frequency_toggle},
                }
            )

        daily_counts_df = pd.DataFrame(daily_counts)

        # Plot
        fig = go.Figure()
        frequency_colors = {
            "bi_weekly": "#1f77b4",
            "monthly": "#ff7f0e",
            "annual": "#2ca02c",
            "prepaid_3mo": "#8B4229",
            "prepaid_6mo": "#BAA052",
            "prepaid_12mo": "#96A682",
            "unknown": "#1A2E31",
        }
        for freq in frequency_toggle:
            if freq in daily_counts_df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=daily_counts_df["date"],
                        y=daily_counts_df[freq],
                        mode="lines",
                        name=freq.replace("_", " ").title(),
                        stackgroup="one",
                        line=dict(color=frequency_colors.get(freq, None)),
                    )
                )

        # Add total line
        total = daily_counts_df[frequency_toggle].sum(axis=1)
        fig.add_trace(
            go.Scatter(
                x=daily_counts_df["date"],
                y=total,
                mode="lines",
                name="Total",
                line=dict(color="#222222", width=2, dash="dash"),
                hovertemplate="Total: %{y}<extra></extra>",
            )
        )

        fig.update_layout(
            title="Active Members Over Time by Payment Frequency",
            showlegend=True,
            height=600,
            xaxis_title="Date",
            yaxis_title="Number of Active Members",
            hovermode="x unified",
        )
        return fig

    # Callback for Membership Attrition and New Membership chart
    @app.callback(
        Output("membership-attrition-new-chart", "figure"),
        [Input("timeframe-toggle", "value")],
    )
    def update_membership_attrition_new_chart(selected_timeframe):
        # Load the membership data
        df = df_memberships.copy()

        # Convert dates to datetime
        df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
        df["start_date"] = df["start_date"].dt.tz_localize(None)
        df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")
        df["end_date"] = df["end_date"].dt.tz_localize(None)

        # Filter to only include dates up to today
        today = pd.Timestamp.now().normalize()

        # Calculate new memberships by period (only up to today)
        df_new = df.dropna(subset=["start_date"]).copy()
        df_new = df_new[df_new["start_date"] <= today]
        df_new["period"] = df_new["start_date"].dt.to_period(selected_timeframe).dt.start_time
        new_memberships = df_new.groupby("period").size().reset_index(name="new_count")

        # Calculate attrition (ended memberships) by period (only up to today)
        df_ended = df.dropna(subset=["end_date"]).copy()
        df_ended = df_ended[df_ended["end_date"] <= today]
        df_ended["period"] = df_ended["end_date"].dt.to_period(selected_timeframe).dt.start_time
        ended_memberships = df_ended.groupby("period").size().reset_index(name="ended_count")

        # Merge the two datasets on period
        combined = pd.merge(new_memberships, ended_memberships, on="period", how="outer").fillna(0)
        combined = combined.sort_values("period")

        # Filter periods to only show up to today
        combined = combined[combined["period"] <= today]

        # Create the figure with two line traces
        fig = go.Figure()

        # Add new memberships line
        fig.add_trace(
            go.Scatter(
                x=combined["period"],
                y=combined["new_count"],
                mode="lines+markers",
                name="New Memberships",
                line=dict(color=chart_colors["secondary"], width=2),  # Gold
                marker=dict(size=6),
            )
        )

        # Add attrition line
        fig.add_trace(
            go.Scatter(
                x=combined["period"],
                y=combined["ended_count"],
                mode="lines+markers",
                name="Membership Attrition",
                line=dict(color=chart_colors["primary"], width=2),  # Rust
                marker=dict(size=6),
            )
        )

        # Update layout
        fig.update_layout(
            title="Membership Attrition & New Membership Over Time",
            showlegend=True,
            height=500,
            xaxis_title="Period",
            yaxis_title="Number of Memberships",
            hovermode="x unified",
            plot_bgcolor=chart_colors["background"],
            paper_bgcolor=chart_colors["background"],
            font_color=chart_colors["text"],
        )

        return fig

    # Callback for New vs Existing Memberships chart
    @app.callback(
        Output("membership-new-vs-existing-chart", "figure"),
        [Input("timeframe-toggle", "value")],
    )
    def update_membership_new_vs_existing_chart(selected_timeframe):
        """
        Show stacked area chart of active memberships split by:
        - New that month (started in that period)
        - Existing (started before that period)
        """
        # Load the membership data
        df = df_memberships.copy()

        # Convert dates to datetime
        df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
        df["start_date"] = df["start_date"].dt.tz_localize(None)
        df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")
        df["end_date"] = df["end_date"].dt.tz_localize(None)

        # Filter to only include dates up to today
        today = pd.Timestamp.now().normalize()

        # Get all periods from first membership start to today
        min_date = df["start_date"].min()
        if pd.isna(min_date):
            # No data, return empty chart
            fig = go.Figure()
            fig.update_layout(title="No membership data available")
            return fig

        # Create a range of periods from min_date to today
        if selected_timeframe == "M":
            periods = pd.date_range(start=min_date, end=today, freq="MS")
        elif selected_timeframe == "W":
            periods = pd.date_range(start=min_date, end=today, freq="W-MON")
        elif selected_timeframe == "Q":
            periods = pd.date_range(start=min_date, end=today, freq="QS")
        else:  # Year
            periods = pd.date_range(start=min_date, end=today, freq="YS")

        # For each period, count active memberships split by new vs existing
        results = []
        for period_start in periods:
            # Calculate period end based on timeframe
            if selected_timeframe == "M":
                period_end = period_start + pd.DateOffset(months=1) - pd.Timedelta(days=1)
            elif selected_timeframe == "W":
                period_end = period_start + pd.Timedelta(days=6)
            elif selected_timeframe == "Q":
                period_end = period_start + pd.DateOffset(months=3) - pd.Timedelta(days=1)
            else:  # Year
                period_end = period_start + pd.DateOffset(years=1) - pd.Timedelta(days=1)

            # Don't go beyond today
            period_end = min(period_end, today)

            # Find active memberships during this period
            # Active = started before or during period AND (no end date OR ended after period start)
            active_mask = (
                (df["start_date"] <= period_end) &
                ((df["end_date"].isna()) | (df["end_date"] >= period_start))
            )
            active_members = df[active_mask]

            # Split into new (started during this period) vs existing (started before)
            new_mask = (
                (active_members["start_date"] >= period_start) &
                (active_members["start_date"] <= period_end)
            )
            new_count = new_mask.sum()
            existing_count = len(active_members) - new_count

            results.append({
                "period": period_start,
                "new_count": new_count,
                "existing_count": existing_count,
                "total_count": len(active_members)
            })

        results_df = pd.DataFrame(results)

        # Create stacked area chart
        fig = go.Figure()

        # Add existing memberships (bottom layer)
        fig.add_trace(
            go.Scatter(
                x=results_df["period"],
                y=results_df["existing_count"],
                mode="lines",
                name="Existing Memberships",
                line=dict(width=0.5, color=chart_colors["primary"]),  # Rust
                stackgroup="one",
                fillcolor=chart_colors["primary"]
            )
        )

        # Add new memberships (top layer)
        fig.add_trace(
            go.Scatter(
                x=results_df["period"],
                y=results_df["new_count"],
                mode="lines",
                name="New That Period",
                line=dict(width=0.5, color=chart_colors["secondary"]),  # Gold
                stackgroup="one",
                fillcolor=chart_colors["secondary"]
            )
        )

        # Update layout
        fig.update_layout(
            title="Active Memberships: New vs Existing",
            showlegend=True,
            height=500,
            xaxis_title="Period",
            yaxis_title="Number of Active Memberships",
            hovermode="x unified",
            plot_bgcolor=chart_colors["background"],
            paper_bgcolor=chart_colors["background"],
            font_color=chart_colors["text"],
        )

        return fig

    # Callback for Youth Teams chart
    @app.callback(
        Output("youth-teams-chart", "figure"), [Input("timeframe-toggle", "value")]
    )
    def update_youth_teams_chart(selected_timeframe):

        # Create a list to store youth team memberships
        youth_memberships = []

        # Process each membership
        for _, membership in df_memberships.iterrows():
            name = str(membership.get("name", "")).lower()
            status = membership.get("status")

            # Only include active memberships
            if status != "ACT":
                continue

            # Determine team type
            team_type = None
            if "recreation" in name or "rec team" in name:
                team_type = "Recreation"
            elif "development" in name or "dev team" in name:
                team_type = "Development"
            elif "competitive" in name or "comp team" in name:
                team_type = "Competitive"

            if team_type:
                start_date = pd.to_datetime(
                    membership.get("start_date"), errors="coerce"
                )
                if pd.notna(start_date):
                    start_date = start_date.tz_localize(None)
                end_date = pd.to_datetime(membership.get("end_date"), errors="coerce")
                if pd.notna(end_date):
                    end_date = end_date.tz_localize(None)

                if not pd.isna(start_date) and not pd.isna(end_date):
                    youth_memberships.append(
                        {
                            "team_type": team_type,
                            "start_date": start_date,
                            "end_date": end_date,
                        }
                    )

        if not youth_memberships:
            fig = px.bar(title="No youth teams data available")
            fig.add_annotation(
                text="No youth teams data available",
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=16),
            )
            return fig

        # Create a DataFrame from youth memberships
        df_youth = pd.DataFrame(youth_memberships)

        # Create a date range from the earliest start date to today
        min_date = df_youth["start_date"].min()
        max_date = datetime.now()
        date_range = pd.date_range(start=min_date, end=max_date, freq="D")

        # Calculate active memberships for each day by team type
        daily_counts = []
        for date in date_range:
            active_memberships = df_youth[
                (df_youth["start_date"] <= date) & (df_youth["end_date"] >= date)
            ]

            counts = active_memberships["team_type"].value_counts().to_dict()
            daily_counts.append(
                {
                    "date": date,
                    "Recreation": counts.get("Recreation", 0),
                    "Development": counts.get("Development", 0),
                    "Competitive": counts.get("Competitive", 0),
                }
            )

        daily_counts_df = pd.DataFrame(daily_counts)

        # Create the stacked line chart
        fig = go.Figure()

        # Define colors for each team type
        team_colors = {
            "Recreation": chart_colors["tertiary"],  # sage
            "Development": chart_colors["secondary"],  # gold
            "Competitive": chart_colors["primary"],  # rust
        }

        # Add a line for each team type
        for team_type in ["Recreation", "Development", "Competitive"]:
            fig.add_trace(
                go.Scatter(
                    x=daily_counts_df["date"],
                    y=daily_counts_df[team_type],
                    mode="lines",
                    name=team_type,
                    stackgroup="one",
                    line=dict(color=team_colors[team_type]),
                )
            )

        # Add total line
        total = daily_counts_df[["Recreation", "Development", "Competitive"]].sum(
            axis=1
        )
        fig.add_trace(
            go.Scatter(
                x=daily_counts_df["date"],
                y=total,
                mode="lines",
                name="Total",
                line=dict(color=chart_colors["text"], width=2, dash="dash"),
                hovertemplate="Total: %{y}<extra></extra>",
            )
        )

        # Update layout
        fig.update_layout(
            title="Youth Teams Membership Over Time",
            showlegend=True,
            height=600,
            xaxis_title="Date",
            yaxis_title="Number of Team Members",
            hovermode="x unified",
            plot_bgcolor=chart_colors["background"],
            paper_bgcolor=chart_colors["background"],
            font_color=chart_colors["text"],
        )

        return fig

    # Callback for Birthday Participants chart
    @app.callback(
        Output("birthday-participants-chart", "figure"),
        [Input("timeframe-toggle", "value")],
    )
    def update_birthday_participants_chart(selected_timeframe):
        # Filter for birthday transactions
        df_filtered = df_combined[df_combined["sub_category"] == "birthday"].copy()
        df_filtered["Date"] = pd.to_datetime(df_filtered["Date"], errors="coerce")
        df_filtered["Date"] = df_filtered["Date"].dt.tz_localize(None)
        df_filtered["date"] = (
            df_filtered["Date"].dt.to_period(selected_timeframe).dt.start_time
        )

        # Group by date and sub_category_detail to count transactions
        birthday_counts = (
            df_filtered.groupby(["date", "sub_category_detail"])
            .size()
            .unstack(fill_value=0)
        )

        # Create the clustered column chart
        fig = go.Figure()

        # Add bars for initial payments
        fig.add_trace(
            go.Bar(
                x=birthday_counts.index,
                y=birthday_counts.get("initial payment", 0),
                name="Initial Payment",
                marker_color=chart_colors["quaternary"],  # dark teal
            )
        )

        # Add bars for second payments
        fig.add_trace(
            go.Bar(
                x=birthday_counts.index,
                y=birthday_counts.get("second payment", 0),
                name="Second Payment",
                marker_color=chart_colors["primary"],  # rust
            )
        )

        # Update layout
        fig.update_layout(
            title="Birthday Party Participants",
            xaxis_title="Date",
            yaxis_title="Number of Transactions",
            barmode="group",
            plot_bgcolor=chart_colors["background"],
            paper_bgcolor=chart_colors["background"],
            font_color=chart_colors["text"],
        )

        return fig

    # Callback for Birthday Revenue chart
    @app.callback(
        Output("birthday-revenue-chart", "figure"), [Input("timeframe-toggle", "value")]
    )
    def update_birthday_revenue_chart(selected_timeframe):
        # Filter for birthday transactions
        df_filtered = df_combined[df_combined["sub_category"] == "birthday"].copy()
        df_filtered["Date"] = pd.to_datetime(df_filtered["Date"], errors="coerce")
        df_filtered["Date"] = df_filtered["Date"].dt.tz_localize(None)
        df_filtered["date"] = (
            df_filtered["Date"].dt.to_period(selected_timeframe).dt.start_time
        )

        # Calculate total revenue by date
        birthday_revenue = (
            df_filtered.groupby("date")["Total Amount"].sum().reset_index()
        )

        # Create the line chart
        fig = px.line(
            birthday_revenue, x="date", y="Total Amount", title="Birthday Party Revenue"
        )

        # Update trace color
        fig.update_traces(line_color=chart_colors["quaternary"])  # dark teal

        # Update layout
        fig.update_layout(
            plot_bgcolor=chart_colors["background"],
            paper_bgcolor=chart_colors["background"],
            font_color=chart_colors["text"],
        )

        return fig

    # Callback for Fitness Revenue chart
    @app.callback(
        Output("fitness-revenue-chart", "figure"), [Input("timeframe-toggle", "value")]
    )
    def update_fitness_revenue_chart(selected_timeframe):
        # Filter for transactions with fitness_amount > 0
        df_filtered = df_combined[df_combined["fitness_amount"] > 0].copy()
        df_filtered["Date"] = pd.to_datetime(df_filtered["Date"], errors="coerce")
        df_filtered["Date"] = df_filtered["Date"].dt.tz_localize(None)
        df_filtered["date"] = (
            df_filtered["Date"].dt.to_period(selected_timeframe).dt.start_time
        )

        # Calculate total fitness revenue by date
        fitness_revenue = (
            df_filtered.groupby("date")["fitness_amount"].sum().reset_index()
        )

        # Create the bar chart
        fig = px.bar(
            fitness_revenue,
            x="date",
            y="fitness_amount",
            title="Fitness Revenue (Classes, Fitness-Only Memberships, Add-ons)"
        )

        # Update trace color
        fig.update_traces(marker_color=chart_colors["secondary"])  # gold

        # Update layout
        fig.update_layout(
            plot_bgcolor=chart_colors["background"],
            paper_bgcolor=chart_colors["background"],
            font_color=chart_colors["text"],
            yaxis_title="Fitness Revenue ($)",
            xaxis_title="Date",
        )

        return fig

    # Callback for Fitness Class Attendance chart
    @app.callback(
        Output("fitness-class-attendance-chart", "figure"), [Input("timeframe-toggle", "value")]
    )
    def update_fitness_class_attendance_chart(selected_timeframe):
        # Filter for fitness class events (HYROX, transformation, strength, etc.)
        # Event types that are fitness-related
        fitness_event_keywords = ['HYROX', 'transformation', 'strength', 'fitness', 'yoga', 'workout']

        df_filtered = df_events.copy()
        df_filtered['event_type_name_lower'] = df_filtered['event_type_name'].str.lower()

        # Filter for fitness events
        fitness_mask = df_filtered['event_type_name_lower'].apply(
            lambda x: any(keyword.lower() in str(x) for keyword in fitness_event_keywords) if pd.notna(x) else False
        )
        df_filtered = df_filtered[fitness_mask]

        # Convert dates
        df_filtered['start_datetime'] = pd.to_datetime(df_filtered['start_datetime'], errors='coerce')
        df_filtered = df_filtered[df_filtered['start_datetime'].notna()]
        df_filtered['start_datetime'] = df_filtered['start_datetime'].dt.tz_localize(None)

        # Group by timeframe
        df_filtered['date'] = (
            df_filtered['start_datetime'].dt.to_period(selected_timeframe).dt.start_time
        )

        # Calculate total attendance (num_reservations) by date
        attendance = (
            df_filtered.groupby('date')['num_reservations'].sum().reset_index()
        )

        # Create the bar chart
        fig = px.bar(
            attendance,
            x='date',
            y='num_reservations',
            title='Fitness Class Attendance (Total Reservations)'
        )

        # Update trace color
        fig.update_traces(marker_color=chart_colors["tertiary"])  # sage green

        # Update layout
        fig.update_layout(
            plot_bgcolor=chart_colors["background"],
            paper_bgcolor=chart_colors["background"],
            font_color=chart_colors["text"],
            yaxis_title='Total Attendance',
            xaxis_title='Date',
        )

        return fig

    # Callback for Marketing Performance chart
    @app.callback(
        Output("marketing-performance-chart", "figure"), [Input("timeframe-toggle", "value")]
    )
    def update_marketing_performance_chart(selected_timeframe):
        # Filter to last 3 months
        df_ads = df_facebook_ads.copy()
        df_ads['date'] = pd.to_datetime(df_ads['date'])
        three_months_ago = df_ads['date'].max() - pd.Timedelta(days=90)
        df_ads = df_ads[df_ads['date'] >= three_months_ago]

        if df_ads.empty:
            fig = px.bar(title="No Facebook Ads data available")
            fig.add_annotation(
                text="No Facebook Ads data available for the last 3 months",
                xref="paper",
                yref="paper",
                x=0.5,
                y=0.5,
                showarrow=False,
                font=dict(size=16),
            )
            return fig

        # Calculate metrics by target/objective
        metrics_data = []

        # Add to Cart metrics
        add_to_cart_spend = df_ads[df_ads['add_to_carts'] > 0]['spend'].sum()
        add_to_cart_count = df_ads['add_to_carts'].sum()
        if add_to_cart_count > 0:
            metrics_data.append({
                'Target': 'Add to Cart',
                'Spend': add_to_cart_spend,
                'Count': add_to_cart_count,
                'Cost per Action': add_to_cart_spend / add_to_cart_count if add_to_cart_count > 0 else 0
            })

        # Purchase metrics
        purchase_spend = df_ads[df_ads['purchases'] > 0]['spend'].sum()
        purchase_count = df_ads['purchases'].sum()
        if purchase_count > 0:
            metrics_data.append({
                'Target': 'Purchase',
                'Spend': purchase_spend,
                'Count': purchase_count,
                'Cost per Action': purchase_spend / purchase_count if purchase_count > 0 else 0
            })

        # Click-through (Link Clicks) metrics
        link_click_spend = df_ads[df_ads['link_clicks'] > 0]['spend'].sum()
        link_click_count = df_ads['link_clicks'].sum()
        if link_click_count > 0:
            metrics_data.append({
                'Target': 'Link Click',
                'Spend': link_click_spend,
                'Count': link_click_count,
                'Cost per Action': link_click_spend / link_click_count if link_click_count > 0 else 0
            })

        # Lead metrics
        lead_spend = df_ads[df_ads['leads'] > 0]['spend'].sum()
        lead_count = df_ads['leads'].sum()
        if lead_count > 0:
            metrics_data.append({
                'Target': 'Lead',
                'Spend': lead_spend,
                'Count': lead_count,
                'Cost per Action': lead_spend / lead_count if lead_count > 0 else 0
            })

        if not metrics_data:
            fig = px.bar(title="No conversion data available")
            fig.add_annotation(
                text="No conversion actions recorded in the last 3 months",
                xref="paper",
                yref="paper",
                x=0.5,
                y=0.5,
                showarrow=False,
                font=dict(size=16),
            )
            return fig

        metrics_df = pd.DataFrame(metrics_data)

        # Create grouped bar chart
        fig = go.Figure()

        # Add bars for Count
        fig.add_trace(go.Bar(
            name='Count',
            x=metrics_df['Target'],
            y=metrics_df['Count'],
            text=metrics_df['Count'].apply(lambda x: f'{int(x)}'),
            textposition='outside',
            marker_color=chart_colors["quaternary"],  # Teal
            yaxis='y',
            offsetgroup=0
        ))

        # Add bars for Spend
        fig.add_trace(go.Bar(
            name='Spend ($)',
            x=metrics_df['Target'],
            y=metrics_df['Spend'],
            text=metrics_df['Spend'].apply(lambda x: f'${x:.0f}'),
            textposition='outside',
            marker_color=chart_colors["secondary"],  # Gold
            yaxis='y2',
            offsetgroup=1
        ))

        # Add bars for Cost per Action
        fig.add_trace(go.Bar(
            name='Cost per Action ($)',
            x=metrics_df['Target'],
            y=metrics_df['Cost per Action'],
            text=metrics_df['Cost per Action'].apply(lambda x: f'${x:.2f}'),
            textposition='outside',
            marker_color=chart_colors["primary"],  # Rust
            yaxis='y3',
            offsetgroup=2
        ))

        # Update layout for multiple y-axes
        fig.update_layout(
            title='Marketing Performance by Target (Last 3 Months)',
            barmode='group',
            plot_bgcolor=chart_colors["background"],
            paper_bgcolor=chart_colors["background"],
            font_color=chart_colors["text"],
            xaxis=dict(title='Target'),
            yaxis=dict(title='Count', side='left'),
            yaxis2=dict(title='Spend ($)', overlaying='y', side='right'),
            yaxis3=dict(title='Cost per Action ($)', overlaying='y', side='right', position=0.92),
            legend=dict(x=0, y=1.1, orientation='h'),
            height=500
        )

        return fig

    # Callback for Camp Sessions chart
    @app.callback(
        Output("camp-sessions-chart", "figure"), [Input("timeframe-toggle", "value")]
    )
    def update_camp_sessions_chart(selected_timeframe):
        # Filter for camp sessions
        camp_data = df_combined[df_combined["sub_category"] == "camps"].copy()

        if camp_data.empty:
            fig = px.bar(title="No camp session data available")
            fig.add_annotation(
                text="No camp session data available",
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=16),
            )
            return fig

        # Convert dates to the selected timeframe and format them nicely
        camp_data["Date"] = pd.to_datetime(camp_data["Date"], errors="coerce")
        camp_data["Date"] = camp_data["Date"].dt.tz_localize(None)
        camp_data["date"] = (
            camp_data["Date"].dt.to_period(selected_timeframe).dt.start_time
        )
        camp_data["formatted_date"] = camp_data["date"].dt.strftime(
            "%b %Y"
        )  # Format as "Jan 2024"

        # Use sub_category_detail directly (already extracted in data pipeline)
        # Fallback to Description if sub_category_detail is missing
        camp_data["session_label"] = camp_data["sub_category_detail"].fillna(
            camp_data["Description"]
        )

        # Clean up the label - remove "Summer Camp " prefix if present
        camp_data["session_label"] = camp_data["session_label"].str.replace(
            "Summer Camp ", "", regex=False
        )

        # Remove "Capitan reservation #xxx: " or "Capitan reservation (xxx): " pattern from labels
        camp_data["session_label"] = camp_data["session_label"].str.replace(
            r"Capitan reservation [#\(]?\d+[\)]?: ", "", regex=True
        )

        # Group by session and purchase period
        camp_counts = (
            camp_data.groupby(["session_label", "formatted_date"])
            .size()
            .reset_index(name="count")
        )

        # Sort session labels alphabetically for consistent ordering
        sorted_sessions = sorted(camp_counts["session_label"].unique())

        # Create stacked bar chart
        fig = px.bar(
            camp_counts,
            x="session_label",
            y="count",
            color="formatted_date",
            title="Camp Session Purchases by Session and Purchase Period",
            category_orders={"session_label": sorted_sessions},
            labels={
                "session_label": "Camp Session",
                "count": "Number of Purchases",
                "formatted_date": "Purchase Period",
            },
            color_discrete_sequence=[
                chart_colors["primary"],  # rust
                chart_colors["secondary"],  # gold
                chart_colors["tertiary"],  # sage
                chart_colors["quaternary"],  # dark teal
                "#8B4229",  # darker rust
                "#BAA052",  # darker gold
                "#96A682",  # darker sage
                "#1A2E31",  # darker teal
            ],
        )

        # Format the date display in the legend
        fig.update_traces(
            hovertemplate="<br>".join(
                [
                    "Session: %{x}",
                    "Purchase Period: %{customdata[0]}",
                    "Number of Purchases: %{y}",
                ]
            ),
            customdata=camp_counts[["formatted_date"]].values,
        )

        fig.update_layout(
            barmode="stack",
            xaxis_title="Camp Session",
            yaxis_title="Number of Purchases",
            legend_title="Purchase Period",
            height=500,
            plot_bgcolor=chart_colors["background"],
            paper_bgcolor=chart_colors["background"],
            font_color=chart_colors["text"],
        )

        return fig

    # Callback for Camp Revenue chart
    @app.callback(
        Output("camp-revenue-chart", "figure"), [Input("timeframe-toggle", "value")]
    )
    def update_camp_revenue_chart(selected_timeframe):
        # Filter for camp sessions
        camp_data = df_combined[df_combined["sub_category"] == "camps"].copy()

        if camp_data.empty:
            fig = px.bar(title="No camp session data available")
            fig.add_annotation(
                text="No camp session data available",
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=16),
            )
            return fig

        # Convert dates to the selected timeframe and group by date
        camp_data["Date"] = pd.to_datetime(camp_data["Date"], errors="coerce")
        camp_data["Date"] = camp_data["Date"].dt.tz_localize(None)
        camp_data["date"] = (
            camp_data["Date"].dt.to_period(selected_timeframe).dt.start_time
        )
        camp_revenue = camp_data.groupby("date")["Total Amount"].sum().reset_index()

        # Create the bar chart
        fig = px.line(
            camp_revenue,
            x="date",
            y="Total Amount",
            title="Camp Session Revenue Over Time",
        )

        # Update trace color
        fig.update_traces(line_color=chart_colors["quaternary"])  # dark teal

        # Update layout
        fig.update_layout(
            plot_bgcolor=chart_colors["background"],
            paper_bgcolor=chart_colors["background"],
            font_color=chart_colors["text"],
        )

        return fig

    # Callback for 90 for 90 Purchase Volume chart
    @app.callback(
        Output("ninety-for-ninety-timeline-chart", "figure"), [Input("timeframe-toggle", "value")]
    )
    def update_ninety_for_ninety_timeline_chart(selected_timeframe):
        """
        Show 90 for 90 purchase volume by week, colored by conversion status.
        Uses member data (df_members) to track unique people and their conversions.
        """
        # Use members data with 90 for 90
        ninety_members = df_members[df_members["is_90_for_90"] == True].copy()

        if ninety_members.empty:
            fig = px.bar(title="No 90 for 90 memberships found")
            fig.add_annotation(
                text="No 90 for 90 memberships found",
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=16),
            )
            return fig

        # Create person identifier from name (temporary until we have owner_id)
        ninety_members["person_id"] = ninety_members["member_first_name"] + " " + ninety_members["member_last_name"]

        # Convert start dates and group by week
        ninety_members["start_date"] = pd.to_datetime(ninety_members["start_date"], errors="coerce")
        ninety_members["start_date"] = ninety_members["start_date"].dt.tz_localize(None)
        ninety_members["week"] = ninety_members["start_date"].dt.to_period("W").dt.start_time

        # For each unique person, determine if they converted
        df_members_copy = df_members.copy()
        df_members_copy["person_id"] = df_members_copy["member_first_name"] + " " + df_members_copy["member_last_name"]

        def check_conversion_status(row):
            person_id = row["person_id"]
            ninety_start_date = row["start_date"]

            # Look for regular memberships for this person that started after their 90 for 90
            regular_memberships = df_members_copy[
                (df_members_copy["person_id"] == person_id) &
                (df_members_copy["is_90_for_90"] == False) &
                (pd.to_datetime(df_members_copy["start_date"], errors="coerce").dt.tz_localize(None) > ninety_start_date)
            ]

            return "Converted" if len(regular_memberships) > 0 else "Not Converted"

        ninety_members["conversion_status"] = ninety_members.apply(check_conversion_status, axis=1)

        # Group by week and conversion status - count unique people per week
        weekly_counts = (
            ninety_members.groupby(["week", "conversion_status"])
            .size()
            .reset_index(name="count")
        )

        # Create stacked bar chart
        fig = px.bar(
            weekly_counts,
            x="week",
            y="count",
            color="conversion_status",
            title="90 for 90 Purchase Volume by Week",
            labels={
                "week": "Week",
                "count": "Number of Memberships",
                "conversion_status": "Status",
            },
            color_discrete_map={
                "Converted": chart_colors["secondary"],  # Gold - success
                "Not Converted": chart_colors["primary"],  # Rust - needs follow-up
            },
            barmode="stack",
        )

        fig.update_layout(
            plot_bgcolor=chart_colors["background"],
            paper_bgcolor=chart_colors["background"],
            font_color=chart_colors["text"],
            height=500,
            xaxis_title="Week",
            yaxis_title="Number of Memberships",
        )

        return fig

    # Callback for 90 for 90 Conversion Summary chart
    @app.callback(
        Output("ninety-for-ninety-summary-chart", "figure"), [Input("timeframe-toggle", "value")]
    )
    def update_ninety_for_ninety_summary_chart(selected_timeframe):
        """
        Show conversion summary for 90 for 90 memberships.
        Uses member data to track unique people.
        """
        # Use members data with 90 for 90
        ninety_members = df_members[df_members["is_90_for_90"] == True].copy()

        if ninety_members.empty:
            fig = px.bar(title="No 90 for 90 memberships found")
            fig.add_annotation(
                text="No 90 for 90 memberships found",
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=16),
            )
            return fig

        # Create person identifier from name
        ninety_members["person_id"] = ninety_members["member_first_name"] + " " + ninety_members["member_last_name"]
        df_members_copy = df_members.copy()
        df_members_copy["person_id"] = df_members_copy["member_first_name"] + " " + df_members_copy["member_last_name"]

        # Get unique people
        unique_person_ids = ninety_members["person_id"].unique()

        # Count conversions
        converted_count = 0
        not_converted_count = 0

        for person_id in unique_person_ids:
            # Get this person's 90 for 90 membership(s)
            person_ninety = ninety_members[ninety_members["person_id"] == person_id]
            # Get the earliest start date of their 90 for 90 membership(s)
            ninety_start_date = pd.to_datetime(person_ninety["start_date"].min(), errors="coerce")
            if pd.notna(ninety_start_date):
                ninety_start_date = ninety_start_date.tz_localize(None)

            # Check if this person got a regular membership that started after their 90 for 90
            regular_memberships = df_members_copy[
                (df_members_copy["person_id"] == person_id) &
                (df_members_copy["is_90_for_90"] == False) &
                (pd.to_datetime(df_members_copy["start_date"], errors="coerce").dt.tz_localize(None) > ninety_start_date)
            ]
            has_regular = len(regular_memberships) > 0

            # Converted = they got a regular membership after their 90 for 90
            if has_regular:
                converted_count += 1
            else:
                not_converted_count += 1

        # Calculate conversion rate
        total = converted_count + not_converted_count
        conversion_rate = (converted_count / total * 100) if total > 0 else 0

        # Create summary data
        summary_data = pd.DataFrame({
            "Status": ["Converted", "Not Converted"],
            "Count": [converted_count, not_converted_count]
        })

        # Create bar chart
        fig = px.bar(
            summary_data,
            x="Status",
            y="Count",
            title=f"90 for 90 Conversion Summary (Conversion Rate: {conversion_rate:.1f}%)",
            color="Status",
            color_discrete_map={
                "Converted": chart_colors["secondary"],  # Gold - success
                "Not Converted": chart_colors["primary"],  # Rust - needs follow-up
            },
        )

        # Add count labels on bars
        fig.update_traces(texttemplate="%{y}", textposition="outside")

        fig.update_layout(
            plot_bgcolor=chart_colors["background"],
            paper_bgcolor=chart_colors["background"],
            font_color=chart_colors["text"],
            height=400,
            showlegend=False,
            yaxis_title="Number of Members",
        )

        return fig
