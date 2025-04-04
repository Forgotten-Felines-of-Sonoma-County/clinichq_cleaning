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
    page_title="Forgotten Felines of Sonoma County Dashboard",
    page_icon="🐈",
    layout="wide"
)

# Load environment variables
load_dotenv()

# Initialize Supabase client with better error handling
def init_connection():
    try:
        # First try to get credentials from Streamlit secrets
        try:
            url = st.secrets["SUPABASE_URL"]
            key = st.secrets["SUPABASE_KEY"]
        except:
            # If not in Streamlit Cloud, try local environment variables
            load_dotenv()  # Load local .env file
            url = os.getenv("SUPABASE_URL")
            key = os.getenv("SUPABASE_KEY")
        
        if not url or not key:
            st.error("""
            Supabase credentials not found. Please ensure you have either:
            
            For local development:
            1. A .env file in your project root with:
               SUPABASE_URL=your-supabase-url
               SUPABASE_KEY=your-supabase-key
               
            OR
            
            For Streamlit Cloud:
            1. Configured secrets in your Streamlit dashboard
            """)
            st.stop()
            
        return create_client(url, key)
    except Exception as e:
        st.error(f"""
        Failed to connect to Supabase. Please check:
        1. Your .env file exists and has the correct credentials
        2. The credentials are properly formatted (no quotes needed)
        3. You're using the correct Supabase service_role key
        
        Error details: {str(e)}
        """)
        st.stop()

# Use the connection
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
    st.title("🐈 Forgotten Felines TNR Dashboard")
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
            line=dict(color='#3498db', width=2),
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
    
    
    
    
    
    # Age Distribution at TNR
    st.subheader("Age Distribution at TNR")
    
    # Prepare age data
    unique_cats = tnr_data.sort_values('date').groupby('microchip').first()
    unique_cats['total_months'] = unique_cats['age_years'] * 12 + unique_cats['age_months']
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Create histogram
    sns.histplot(data=unique_cats, 
                x='total_months',
                bins=40,  # Adjust number of bins
                color='#3498db',  # Match the blue color scheme
                ax=ax)
    
    # Set x-axis limit to 0-200 months
    ax.set_xlim(0, 200)
    
    # Customize the plot
    ax.set_title('Age Distribution of Cats at Time of TNR', pad=20)
    ax.set_xlabel('Age (Months)')
    ax.set_ylabel('Number of Cats')
    
    # Add grid for better readability
    ax.grid(True, linestyle='--', alpha=0.7)
    
    # Calculate and add statistics for cats within visible range (0-200 months)
    visible_data = unique_cats[unique_cats['total_months'] <= 200]
    stats = visible_data['total_months'].describe()
    
    # Add text annotations for key statistics
    annotation_text = (
        f"Median Age: {stats['50%']:.1f} months\n"
        f"Mean Age: {stats['mean']:.1f} months\n"
        f"Cats shown: {len(visible_data):,} ({len(visible_data)/len(unique_cats)*100:.1f}%)"
    )
    
    # Add text box with statistics
    plt.text(0.95, 0.95, 
             annotation_text,
             transform=ax.transAxes,
             bbox=dict(facecolor='white', alpha=0.8, edgecolor='none'),
             va='top',
             ha='right')
    
    plt.tight_layout()
    st.pyplot(fig)
    
    # Add explanatory text
    st.markdown("""
    **Note:** This visualization shows the age distribution of cats at their time of TNR, limited to 0-200 months 
    (approximately 0-16.7 years) for better visibility of the main distribution. Some cats older than this range 
    may not be shown in the graph.
    """)
    
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
    
    
    

# First (all-time) Top 10 Neighborhoods visualization
    st.subheader("Top 10 Neighborhoods by TNR Count")
    
    # Get top 10 neighborhoods and sort
    tnr_by_postcode = tnr_data.groupby('postcode').size()
    top_10_postcodes = tnr_by_postcode.nlargest(10).sort_values(ascending=False)
    
    # Create the visualization
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Create horizontal bar plot with blue color
    sns.barplot(x=top_10_postcodes.values, 
                y=top_10_postcodes.index, 
                palette=sns.color_palette('mako')[:-1],  # Changed to blue
                ax=ax)
    
    # Customize the plot
    ax.set_title('Top 10 Neighborhoods by TNR Count', pad=20)
    ax.set_xlabel('Number of TNR Procedures')
    ax.set_ylabel('Postcode')
    
    # Add value labels on the bars
    for i, v in enumerate(top_10_postcodes.values):
        ax.text(v, i, f' {int(v)}', va='center')
    
    plt.tight_layout()
    st.pyplot(fig)

    # Second (yearly) Top 10 Neighborhoods visualization
    st.subheader("Top 10 Neighborhoods by TNR Count (By Year)")
    
    # Get min and max years from the data
    tnr_data['year'] = tnr_data['date'].dt.year
    min_year = tnr_data['year'].min()
    max_year = tnr_data['year'].max()
    
    # Create year selector with dropdown
    years_list = list(range(int(min_year), int(max_year) + 1))
    selected_year = st.selectbox(
        "Select Year",
        years_list,
        index=len(years_list) - 1  # Default to most recent year
    )
    
    # Filter data for selected year
    year_data = tnr_data[tnr_data['year'] == selected_year]
    
    # Get top 10 neighborhoods for selected year
    year_tnr_by_postcode = year_data.groupby('postcode').size()
    year_top_10_postcodes = year_tnr_by_postcode.nlargest(10).sort_values(ascending=False)
    
    # Create the visualization
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Create horizontal bar plot with blue color
    sns.barplot(x=year_top_10_postcodes.values, 
                y=year_top_10_postcodes.index, 
                palette=sns.color_palette('mako')[::-1], 
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