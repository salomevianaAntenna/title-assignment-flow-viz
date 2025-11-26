#!/usr/bin/env python3
"""
Title Assignment Flow Visualizer
Sankey diagram showing service flow through the pipeline stages
"""

import streamlit as st
import plotly.graph_objects as go
from google.cloud import bigquery
import pandas as pd

st.set_page_config(
    page_title="Title Assignment Flow Visualizer",
    page_icon="📊",
    layout="wide"
)

@st.cache_resource
def get_bq_client():
    try:
        if "gcp_service_account" in st.secrets:
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
            return bigquery.Client(credentials=credentials, project=credentials.project_id)
    except FileNotFoundError:
        pass
    return bigquery.Client()

client = get_bq_client()

SERVICE_COLORS = {
    'NULL': '#C0C0C0',
    'Linear TV': '#E74C3C',
    'Unknown': '#95A5A6',
    'Paramount+': '#0064FF',
    'Netflix': '#E50914',
    'Amazon Prime': '#00A8E1',
    'Hulu': '#1CE783',
    'Disney+': '#113CCF',
    'Apple TV+': '#000000',
    'HBO Max': '#9D4EDD',
    'Google Play Store': '#4285F4',
    'Dish': '#F77F00',
    'Xfinity Stream': '#990000',
    'YouTube': '#FF0000',
    'YouTube Premium': '#FF0000',
    'YouTube TV': '#FF0000',
    'Plex': '#E5A00D',
    'Fubo': '#FF6600',
    'Peacock': '#000000',
    'Microsoft Movies & TV': '#00A4EF',
    'Spectrum On Demand': '#0476D9',
    'Vudu': '#0088CC',
    'Optimum TV': '#002D5C',
    'Sling TV': '#0061FF',
    'CBS': '#000080',
}

def get_service_color(service):
    return SERVICE_COLORS.get(service, '#7F8C8D')


@st.cache_data(ttl=1800)
def query_sankey_data(_client, dataset, month, title_id, min_confidence):
    query = f"""
    WITH
    sessions_to_analyze AS (
        SELECT smba_id, content_id, start_time, title_id, season, start_month,
               old_service, old_content_type, service, content_type, duration
        FROM `antenna-reporting.{dataset}.session_raw`
        WHERE start_month = '{month}' AND duration > 30
          AND (app_probability_score >= {min_confidence} OR {min_confidence} = 0 OR content_type = "Linear TV")
          AND (title_id = '{title_id}' OR '{title_id}' = '')
    ),
    processed_stage AS (
        SELECT smba_id, content_id, start_time, service
        FROM `antenna-reporting.{dataset}.session_processed`
        WHERE start_month = '{month}' AND (title_id = '{title_id}' OR '{title_id}' = '')
    ),
    apv_stage AS (
        SELECT smba_id, content_id, start_time, service
        FROM `antenna-reporting.{dataset}.session_apv_assign`
        WHERE start_month = '{month}' AND (title_id = '{title_id}' OR '{title_id}' = '')
    ),
    unknown_phase1 AS (
        SELECT smba_id, start_month, title_id, season, service
        FROM `antenna-reporting.{dataset}.title_update_specific`
        WHERE start_month = '{month}' AND (title_id = '{title_id}' OR '{title_id}' = '')
    ),
    unknown_phase2 AS (
        SELECT smba_id, start_month, title_id, season, service
        FROM `antenna-reporting.{dataset}.title_update_generic`
        WHERE start_month = '{month}' AND (title_id = '{title_id}' OR '{title_id}' = '')
    ),
    title_app_dist AS (
        SELECT smba_id, start_month, title_id, season, service,
               CASE WHEN EXISTS (
                   SELECT 1 FROM `antenna-reporting.{dataset}.app_dist` ad
                   WHERE ad.smba_id = tad.smba_id
                   AND ad.start_month = tad.start_month
                   AND ad.service = tad.service
               ) THEN 'phase2a' ELSE 'phase2b' END as phase2_type
        FROM `antenna-reporting.{dataset}.title_app_dist` tad
        WHERE start_month = '{month}' AND (title_id = '{title_id}' OR '{title_id}' = '')
    ),
    unknown_stage AS (
        SELECT smba_id, content_id, start_time, title_id, season, start_month, service
        FROM `antenna-reporting.{dataset}.session_unknown_assign`
        WHERE start_month = '{month}' AND (title_id = '{title_id}' OR '{title_id}' = '')
    ),
    session_paths AS (
        SELECT
            CASE WHEN r.old_service IS NULL AND r.old_content_type = 'Linear TV' THEN 'Linear TV'
                 ELSE COALESCE(r.old_service, 'NULL') END as stage1,
            CASE WHEN r.service IS NULL AND r.content_type = 'Linear TV' THEN 'Linear TV'
                 ELSE COALESCE(r.service, 'NULL') END as stage2,
            COALESCE(p.service, 'NULL') as stage3,
            COALESCE(apv.service, 'NULL') as stage4,
            COALESCE(u.service, 'NULL') as stage5,
            CASE
                WHEN u.service IS NOT NULL AND u1.service = u.service THEN 'phase1'
                WHEN u.service IS NOT NULL AND u2.service = u.service AND tad.phase2_type = 'phase2a' THEN 'phase2a'
                WHEN u.service IS NOT NULL AND u2.service = u.service AND tad.phase2_type = 'phase2b' THEN 'phase2b'
                ELSE 'none'
            END as assignment_phase,
            COUNT(*) as session_count,
            SUM(r.duration) / 60.0 as total_minutes
        FROM sessions_to_analyze r
        LEFT JOIN processed_stage p ON r.smba_id = p.smba_id AND r.content_id = p.content_id AND r.start_time = p.start_time
        LEFT JOIN apv_stage apv ON r.smba_id = apv.smba_id AND r.content_id = apv.content_id AND r.start_time = apv.start_time
        LEFT JOIN unknown_stage u ON r.smba_id = u.smba_id AND r.content_id = u.content_id AND r.start_time = u.start_time
        LEFT JOIN unknown_phase1 u1 ON u.smba_id = u1.smba_id AND u.title_id = u1.title_id
            AND u.season IS NOT DISTINCT FROM u1.season AND u.start_month = u1.start_month
        LEFT JOIN unknown_phase2 u2 ON u.smba_id = u2.smba_id AND u.title_id = u2.title_id
            AND u.season IS NOT DISTINCT FROM u2.season AND u.start_month = u2.start_month
        LEFT JOIN title_app_dist tad ON u.smba_id = tad.smba_id AND u.title_id = tad.title_id
            AND u.season IS NOT DISTINCT FROM tad.season AND u.start_month = tad.start_month
            AND u.service = tad.service
        GROUP BY 1, 2, 3, 4, 5, 6
        HAVING session_count > 10 OR stage2 = 'Linear TV'
    )
    SELECT stage1, stage2, stage3, stage4, stage5, assignment_phase, ROUND(total_minutes, 0) as value
    FROM session_paths ORDER BY value DESC
    """

    df = _client.query(query).to_dataframe()
    return df


def create_sankey_figure(df, top_n):
    df = df.head(top_n).copy()

    stages = ['stage1', 'stage2', 'stage3', 'stage4', 'stage5']
    stage_labels = [
        'Stage 1: Raw Input',
        'Stage 2: Linear Reassignment<br>(MVPD + Daily Detection)',
        'Stage 3: PPV Detection<br>& Freevee Isolation',
        'Stage 4: APV Addon<br>& Disney/Hulu Assignment',
        'Stage 5: Unknown Resolution<br>(User-Specific & Population)'
    ]

    phase_order = {'phase1': 0, 'phase2a': 1, 'phase2b': 2, 'none': 3}
    df['phase_order'] = df['assignment_phase'].map(phase_order)
    df = df.sort_values(['phase_order', 'value'], ascending=[True, False])
    df = df.drop('phase_order', axis=1)

    all_nodes, node_dict, node_y_positions, node_x_positions = [], {}, [], []
    node_idx = 0

    for stage_idx, stage in enumerate(stages):
        if stage_idx == 0:
            stage1_totals = df.groupby(stage)['value'].sum().sort_values(ascending=False)
            n_nodes = len(stage1_totals)

            for position, (node_val, total) in enumerate(stage1_totals.items()):
                node_key = f"{stage}_{node_val}"
                all_nodes.append(str(node_val))
                node_dict[node_key] = node_idx
                node_x_positions.append(0.0)
                y_pos = 0.05 + (position * 0.9 / max(n_nodes - 1, 1))
                node_y_positions.append(y_pos)
                node_idx += 1
        else:
            stage_totals = df.groupby(stage)['value'].sum().sort_values(ascending=False)
            n_nodes = len(stage_totals)

            for position, (node_val, total) in enumerate(stage_totals.items()):
                node_key = f"{stage}_{node_val}"
                all_nodes.append(str(node_val))
                node_dict[node_key] = node_idx
                node_x_positions.append(stage_idx / (len(stages) - 1))
                y_pos = 0.05 + (position * 0.9 / max(n_nodes - 1, 1))
                node_y_positions.append(y_pos)
                node_idx += 1

    sources, targets, values, link_colors = [], [], [], []

    for i in range(len(stages) - 1):
        source_col, target_col = stages[i], stages[i + 1]

        for _, row in df.iterrows():
            source_key = f"{source_col}_{row[source_col]}"
            target_key = f"{target_col}_{row[target_col]}"

            sources.append(node_dict[source_key])
            targets.append(node_dict[target_key])
            values.append(row['value'])

            assignment_phase = row['assignment_phase']
            source_service = str(row[source_col])

            if i == 3 and source_service == 'Unknown' and assignment_phase == 'phase1':
                link_colors.append('rgba(46, 204, 113, 0.5)')
            elif i == 3 and source_service == 'Unknown' and assignment_phase == 'phase2a':
                link_colors.append('rgba(52, 152, 219, 0.5)')
            elif i == 3 and source_service == 'Unknown' and assignment_phase == 'phase2b':
                link_colors.append('rgba(155, 89, 182, 0.5)')
            else:
                link_colors.append('rgba(189, 195, 199, 0.3)')

    colors = [get_service_color(node) for node in all_nodes]
    bold_labels = [f'<span style="color:black">{label}</span>' for label in all_nodes]

    fig = go.Figure(data=[go.Sankey(
        arrangement='snap',
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="rgba(0,0,0,0)", width=0),
            label=bold_labels,
            color=colors,
            x=node_x_positions,
            y=node_y_positions,
            align='right',
            hovertemplate='%{label}<extra></extra>'
        ),
        link=dict(source=sources, target=targets, value=values, color=link_colors),
        textfont=dict(color='black', family='Arial', size=11)
    )])

    annotations = []
    for idx, label in enumerate(stage_labels):
        annotations.append(dict(
            x=idx / (len(stages) - 1), y=1.015, xref='paper', yref='paper',
            text=f'<b>{label}</b>', showarrow=False,
            font=dict(size=10, color='#2C3E50', family='Arial'),
            xanchor='center', yanchor='bottom'
        ))

    annotations.extend([
        dict(x=1.02, y=0.54, xref='paper', yref='paper', showarrow=False,
             text='<b>Phase 1:</b> User-Specific History', xanchor='left',
             font=dict(size=10, color='#2ECC71', family='Arial')),
        dict(x=1.02, y=0.515, xref='paper', yref='paper', showarrow=False,
             text='(Individual viewing for this title/season)', xanchor='left',
             font=dict(size=8, color='#666666', family='Arial')),
        dict(x=1.02, y=0.48, xref='paper', yref='paper', showarrow=False,
             text='<b>Phase 2A:</b> User Preferences + Title Patterns', xanchor='left',
             font=dict(size=10, color='#3498DB', family='Arial')),
        dict(x=1.02, y=0.455, xref='paper', yref='paper', showarrow=False,
             text='(General user behavior + population data)', xanchor='left',
             font=dict(size=8, color='#666666', family='Arial')),
        dict(x=1.02, y=0.42, xref='paper', yref='paper', showarrow=False,
             text='<b>Phase 2B:</b> Population Patterns Only', xanchor='left',
             font=dict(size=10, color='#9B59B6', family='Arial')),
        dict(x=1.02, y=0.395, xref='paper', yref='paper', showarrow=False,
             text='(No user data available)', xanchor='left',
             font=dict(size=8, color='#666666', family='Arial'))
    ])

    fig.update_layout(
        font=dict(size=10, family='Arial', color='#000000'),
        height=800,
        plot_bgcolor='white',
        paper_bgcolor='white',
        annotations=annotations,
        margin=dict(t=60, b=50, l=50, r=250)
    )

    return fig


st.title("📊 Title Assignment Flow Visualizer")
st.markdown("Interactive Sankey diagram showing service flow through the title assignment pipeline")

st.sidebar.header("Filters")

selected_month = st.sidebar.text_input(
    "Month (YYYY-MM-DD)",
    value="2025-08-01",
    help="Format: YYYY-MM-DD (e.g., 2025-08-01)"
)

dataset = st.sidebar.selectbox(
    "Dataset",
    ["bo_title", "salo_epg_eligibility"],
    index=0
)

selected_title = st.sidebar.text_input(
    "Title ID",
    value="",
    help="Leave empty for all titles"
)

min_confidence = st.sidebar.number_input(
    "Min SMBA Confidence",
    min_value=0.0,
    max_value=1.0,
    value=0.0,
    step=0.1,
    help="Minimum app_probability_score"
)

if st.sidebar.button("Generate Visualization", type="primary"):
    with st.spinner("Querying BigQuery..."):
        try:
            top_n = 30

            df = query_sankey_data(
                client, dataset, selected_month, selected_title, min_confidence
            )

            if df.empty:
                st.warning("No data found for the selected filters")
            else:
                st.success(f"Retrieved {len(df)} flows (showing top {top_n})")

                with st.spinner("Creating visualization..."):
                    fig = create_sankey_figure(df, top_n)

                    st.plotly_chart(
                        fig,
                        use_container_width=True,
                        config={
                            'displayModeBar': True,
                            'displaylogo': False,
                            'modeBarButtonsToRemove': ['select2d', 'lasso2d', 'pan2d']
                        }
                    )

                with st.expander("📋 View Raw Data"):
                    st.dataframe(df.head(top_n))

        except Exception as e:
            st.error(f"Error: {str(e)}")
            st.exception(e)

st.sidebar.markdown("---")
st.sidebar.markdown("**Title Assignment Pipeline**")
st.sidebar.markdown("Built with Streamlit + Plotly + BigQuery")
