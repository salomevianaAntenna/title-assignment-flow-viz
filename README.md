# Title Assignment Flow Visualizer

Interactive Sankey diagram for visualizing service flow through the title assignment pipeline.

## Features

- Interactive flow visualization across 5 pipeline stages
- Filter by month, dataset, title, and confidence threshold
- Color-coded phases for unknown resolution (Phase 1, 2A, 2B)
- BigQuery integration for live data
- Responsive design with fullscreen mode

## Pipeline Stages

1. **Raw Input** - Initial service classification
2. **Linear Reassignment** - MVPD + Daily detection
3. **PPV Detection** - PPV detection & Freevee isolation
4. **APV Addon** - APV addon & Disney/Hulu assignment
5. **Unknown Resolution** - User-specific & population-based resolution

## Unknown Resolution Phases

- **Phase 1** (Green): User-specific history for this title/season
- **Phase 2A** (Blue): User preferences + title patterns
- **Phase 2B** (Purple): Population patterns only (no user data)

## Local Usage

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

## Deployment

This app is deployed on Streamlit Cloud and connects to BigQuery using service account credentials stored in Streamlit secrets.

### Required Secrets

Add the following to `.streamlit/secrets.toml` (local) or Streamlit Cloud secrets (production):

```toml
[gcp_service_account]
type = "service_account"
project_id = "your-project-id"
private_key_id = "..."
private_key = "..."
client_email = "..."
client_id = "..."
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url = "..."
```

## Tech Stack

- **Streamlit** - Web framework
- **Plotly** - Interactive visualizations
- **BigQuery** - Data warehouse
- **Pandas** - Data manipulation
