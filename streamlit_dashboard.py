import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from supabase import create_client
import os
from dotenv import load_dotenv
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Page configuration
st.set_page_config(
    page_title="TNR Program Dashboard",
    page_icon="🐈",
    layout="wide"
)

# Load environment variables
load_dotenv()

# Initialize Supabase client
@st.cache_resource
def init_connection():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    return create_client(url, key)

supabase = init_connection()

# Function to fetch all TNR appointments with pagination
@st.cache_data
def fetch_all_tnr_appointments():
    all_appointments = []
    page_size = 1000
    start = 0
    
    while True:
        response = supabase.table('appointments').select(
            '*'
        ).eq('appointment_type', 'Spay Or Neuter').range(start, start + page_size - 1).execute()
        
        batch_df = pd.DataFrame(response.data)
        
        if len(batch_df) == 0:
            break
            
        if len(batch_df) > 0:
            microchips = batch_df['microchip'].unique()
            # Add age columns to the cats table select
            cats_response = supabase.table('cats').select(
                'microchip',
                'postcode',
                'age_years',
                'age_months'
            ).in_('microchip', microchips).execute()
            
            cats_df = pd.DataFrame(cats_response.data)
            
            # Merge appointments with cat data
            merged_df = pd.merge(batch_df, cats_df, on='microchip', how='left')
            all_appointments.append(merged_df)
            
        start += page_size
        
    return pd.concat(all_appointments) if all_appointments else pd.DataFrame()

# Function to fetch all cats with pagination
@st.cache_data
def fetch_all_cats():
    all_cats = []
    page_size = 1000
    start = 0
    
    while True:
        response = supabase.table('cats').select('microchip').range(start, start + page_size - 1).execute()
        batch_df = pd.DataFrame(response.data)
        
        if len(batch_df) == 0:
            break
            
        all_cats.append(batch_df)
        start += page_size
        
    return pd.concat(all_cats) if all_cats else pd.DataFrame()

# Main app
def main():
    # Header
    st.title("🐈 TNR Program Dashboard")
    st.markdown("Trap-Neuter-Return program metrics for Forgotten Felines of Sonoma County (data taken via ClincHQ)")
    
    # Load data with loading spinner
    with st.spinner("Loading TNR data..."):
        tnr_data = fetch_all_tnr_appointments()
        cats_df = fetch_all_cats()
    
    # Convert dates
    tnr_data['date'] = pd.to_datetime(tnr_data['date'], utc=True)
    
    # After loading data and before metrics row
    avg_years, avg_months = calculate_average_age(tnr_data)
    
    # Update metrics row to include average age
    col1, col2, col3, col4 = st.columns(4)
    
    total_cats = len(cats_df['microchip'].unique())
    total_altered = len(tnr_data['microchip'].unique())
    alteration_rate = (total_altered / total_cats * 100) if total_cats > 0 else 0
    
    with col1:
        st.metric("Total Cats Tracked", f"{total_cats:,}")
    with col2:
        st.metric("Total Cats Altered", f"{total_altered:,}")
    with col3:
        st.metric("Alteration Rate", f"{alteration_rate:.1f}%")
    with col4:
        st.metric("Average Age at TNR", 
                 f"{avg_years}y {avg_months}m",
                 help="Average age of cats when they undergo TNR")
    
    # TNR Progress Over Time - Interactive
    st.subheader("TNR Progress Over Time")
    
    # Prepare data
    sorted_data = tnr_data.sort_values('date')
    sorted_data['cumulative_count'] = range(1, len(sorted_data) + 1)
    
    # Create interactive line plot
    fig_progress = go.Figure()
    fig_progress.add_trace(
        go.Scatter(
            x=sorted_data['date'],
            y=sorted_data['cumulative_count'],
            mode='lines',
            name='Cumulative TNR',
            line=dict(color='#2ecc71', width=2),
            hovertemplate='Date: %{x}<br>Total TNR: %{y}<extra></extra>'
        )
    )
    
    fig_progress.update_layout(
        title='Cumulative TNR Procedures',
        xaxis_title='Date',
        yaxis_title='Number of Procedures',
        hovermode='x unified',
        height=500,
        showlegend=False
    )
    
    st.plotly_chart(fig_progress, use_container_width=True)
    
    
    
    
    
    # Age Distribution - Interactive
    st.subheader("Age Distribution at TNR")
    
    # Prepare age data
    unique_cats = tnr_data.sort_values('date').groupby('microchip').first()
    unique_cats['total_months'] = unique_cats['age_years'] * 12 + unique_cats['age_months']
    
    # Create interactive histogram
    fig_age = go.Figure()
    fig_age.add_trace(
        go.Histogram(
            x=unique_cats['total_months'],
            nbinsx=30,
            marker_color='#3498db',
            hovertemplate='Age: %{x:.1f} months<br>Count: %{y}<extra></extra>'
        )
    )
    
    fig_age.update_layout(
        xaxis_title='Age (months)',
        yaxis_title='Number of Cats',
        height=500,
        showlegend=False,
        bargap=0.1
    )
    
    st.plotly_chart(fig_age, use_container_width=True)
    
    # Monthly Trends - New Interactive Chart
    st.subheader("Monthly TNR Trends")
    
    monthly_counts = tnr_data.groupby(pd.Grouper(key='date', freq='M')).size().reset_index()
    monthly_counts.columns = ['date', 'count']
    
    fig_monthly = go.Figure()
    fig_monthly.add_trace(
        go.Scatter(
            x=monthly_counts['date'],
            y=monthly_counts['count'],
            mode='lines+markers',
            line=dict(color='#3498db', width=2),
            marker=dict(size=8),
            hovertemplate='Month: %{x|%B %Y}<br>TNR Count: %{y}<extra></extra>'
        )
    )
    
    fig_monthly.update_layout(
        title='Monthly TNR Procedures',
        xaxis_title='Month',
        yaxis_title='Number of Procedures',
        height=500,
        showlegend=False,
        hovermode='x unified'
    )
    
    st.plotly_chart(fig_monthly, use_container_width=True)
    
    # Monthly Statistics
    st.subheader("Monthly Statistics")
    monthly_counts = tnr_data.groupby(pd.Grouper(key='date', freq='M')).size()
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Average Monthly Procedures", f"{monthly_counts.mean():.1f}")
    with col2:
        st.metric("Highest Month", 
                 f"{monthly_counts.max()} procedures",
                 f"({monthly_counts.idxmax().strftime('%B %Y')})")
    
    
    

# Top Neighborhoods visualization
    st.subheader("Top 10 Neighborhoods by TNR Count")
    
    # Get top 10 neighborhoods and sort by count (ascending=False for highest at top)
    tnr_by_postcode = tnr_data.groupby('postcode').size()
    top_10_postcodes = tnr_by_postcode.nlargest(10).sort_values(ascending=False)  # Changed to False
    
    # Create the visualization
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Create horizontal bar plot
    sns.barplot(x=top_10_postcodes.values, 
                y=top_10_postcodes.index, 
                palette='viridis',
                ax=ax)
    
    # Customize the plot
    ax.set_title('Top 10 Neighborhoods by TNR Count (All Time)', pad=20)
    ax.set_xlabel('Number of TNR Procedures')
    ax.set_ylabel('Postcode')
    
    # Add value labels on the bars
    for i, v in enumerate(top_10_postcodes.values):
        ax.text(v, i, f' {int(v)}', va='center')
    
    plt.tight_layout()
    st.pyplot(fig)
    
    
    st.subheader("Top 10 Neighborhoods by TNR Count (By Year)")
    
    # Get min and max years from the data
    tnr_data['year'] = tnr_data['date'].dt.year
    min_year = tnr_data['year'].min()
    max_year = tnr_data['year'].max()
    
    # Create year selector with radio buttons
    years_list = list(range(int(min_year), int(max_year) + 1))
    selected_year = st.radio(
        "Select Year",
        years_list,
        horizontal=True,
        index=len(years_list) - 1  # Set default to last (most recent) year
    )
    
    # Filter data for selected year
    year_data = tnr_data[tnr_data['year'] == selected_year]
    
    # Get top 10 neighborhoods for selected year
    year_tnr_by_postcode = year_data.groupby('postcode').size()
    year_top_10_postcodes = year_tnr_by_postcode.nlargest(10).sort_values(ascending=False)
    
    # Create the visualization
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Create horizontal bar plot
    sns.barplot(x=year_top_10_postcodes.values, 
                y=year_top_10_postcodes.index, 
                palette='viridis',
                ax=ax)
    
    # Customize the plot
    ax.set_title(f'Top 10 Neighborhoods by TNR Count ({selected_year})', pad=20)
    ax.set_xlabel('Number of TNR Procedures')
    ax.set_ylabel('Postcode')
    
    # Add value labels on the bars
    for i, v in enumerate(year_top_10_postcodes.values):
        ax.text(v, i, f' {int(v)}', va='center')
    
    plt.tight_layout()
    st.pyplot(fig)
    
    # Add year statistics
    total_year_procedures = len(year_data)
    year_unique_cats = len(year_data['microchip'].unique())
    
    # Create three columns for metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            f"Total TNR Procedures ({selected_year})",
            f"{total_year_procedures:,}"
        )
    
    with col2:
        st.metric(
            f"Unique Cats Altered ({selected_year})",
            f"{year_unique_cats:,}"
        )
    
    with col3:
        st.metric(
            "Top Neighborhood",
            f"{year_top_10_postcodes.index[0]}",
            f"{int(year_top_10_postcodes.values[0])} procedures"
        )
    
    # Add year-over-year comparison if not earliest year
    if selected_year > min_year:
        st.subheader("Year-over-Year Comparison")
        
        # Get previous year data
        prev_year_data = tnr_data[tnr_data['year'] == (selected_year - 1)]
        prev_year_tnr = prev_year_data.groupby('postcode').size()
        
        # Calculate changes
        yoy_change = total_year_procedures - len(prev_year_data)
        yoy_percent = (yoy_change / len(prev_year_data) * 100) if len(prev_year_data) > 0 else 0
        
        st.write(f"Change from {selected_year-1}: {yoy_change:+,} procedures ({yoy_percent:+.1f}%)")
        
        # Show neighborhood changes
        prev_top_10 = set(prev_year_tnr.nlargest(10).index)
        current_top_10 = set(year_top_10_postcodes.index)
        
        new_to_top_10 = current_top_10 - prev_top_10
        if new_to_top_10:
            st.write("📈 **New to Top 10 this year:**")
            for postcode in new_to_top_10:
                st.write(f"- Postcode {postcode}: {year_tnr_by_postcode[postcode]} TNR procedures")

    # Data Download Section
    st.subheader("Download Data")
    if st.button("Prepare TNR Data for Download"):
        csv = tnr_data.to_csv(index=False)
        st.download_button(
            label="Download TNR Data as CSV",
            data=csv,
            file_name="tnr_data.csv",
            mime="text/csv"
        )


def calculate_average_age(df):
    # Convert years and months to total months
    df['total_months'] = (df['age_years'] * 12 + df['age_months']).fillna(0)
    
    # Get unique cats (using first TNR appointment for each cat)
    unique_cats = df.sort_values('date').groupby('microchip').first()
    
    # Calculate average age in months
    avg_months = unique_cats['total_months'].mean()
    
    # Convert back to years and months
    avg_years = int(avg_months // 12)
    avg_remaining_months = round(avg_months % 12, 1)
    
    return avg_years, avg_remaining_months

if __name__ == "__main__":
    main() 