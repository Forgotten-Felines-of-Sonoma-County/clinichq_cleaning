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
            # Ensure last_updated is included
            cats_response = supabase.table('cats').select(
                'microchip',
                'postcode',
                'age_years',
                'age_months',
                'last_updated'  # Include last_updated
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
    avg_years, avg_months = calculate_age_at_tnr(tnr_data, cats_df)
    
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
    st.subheader("Age Distribution at Time of TNR")
    
    # Prepare the data
    age_dist_data = prepare_age_distribution_data(tnr_data)
    
    # Create age distribution counts
    age_counts = age_dist_data['age_at_tnr_months'].value_counts().reset_index()
    age_counts.columns = ['age_month', 'count']
    age_counts = age_counts.sort_values('age_month')
    
    # Convert months to years and months for display
    age_counts['years'] = age_counts['age_month'] // 12
    age_counts['months'] = age_counts['age_month'] % 12
    age_counts['age_label'] = age_counts.apply(lambda x: f"{x['years']}y {x['months']}m", axis=1)
    
    # Create interactive histogram
    fig = px.bar(
        age_counts, 
        x='age_month',
        y='count',
        labels={'age_month': 'Age (months)', 'count': 'Number of Cats'},
        color='count',
        color_continuous_scale='Blues',
        hover_data=['age_label', 'count']
    )
    
    # Customize hover template
    fig.update_traces(
        hovertemplate='<b>Age:</b> %{customdata[0]}<br><b>Count:</b> %{y}<extra></extra>'
    )
    
    # Set layout with x-axis limit of 200 months
    fig.update_layout(
        xaxis_title='Age (months)',
        yaxis_title='Number of Cats',
        height=500,
        xaxis=dict(
            range=[0, 200],  # Set x-axis limit to 200 months
            tickmode='linear',
            tick0=0,
            dtick=12,  # Major ticks every 12 months (1 year)
            ticktext=[f"{i}y" for i in range(0, 17)],  # 0 to 16 years (200 months is ~16.7 years)
            tickvals=[i*12 for i in range(0, 17)]
        )
    )
    
    # Add vertical lines every year for better readability (up to 16 years)
    for i in range(1, 17):
        fig.add_shape(
            type="line",
            x0=i*12, x1=i*12,
            y0=0, y1=1,
            yref="paper",
            line=dict(color="gray", width=1, dash="dot")
        )
    
    # Add note about limited x-axis
    st.markdown("""
    **Note:** The chart displays ages up to 200 months (~16.7 years) for better visibility. 
    Some older cats may not be shown on this chart but are included in the statistical calculations.
    """)
    
    # Add statistics for cats beyond the visible range
    cats_beyond_range = len(age_dist_data[age_dist_data['age_at_tnr_months'] > 200])
    if cats_beyond_range > 0:
        st.markdown(f"*There are {cats_beyond_range} cats older than 200 months not displayed on this chart.*")
    
    # Show the plot
    st.plotly_chart(fig, use_container_width=True)
    
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
    
    
    

    # Merged Top 10 Neighborhoods Visualization with All Time option
    st.subheader("Top 10 Neighborhoods by TNR Count")

    # Get min and max years from the data
    tnr_data['year'] = tnr_data['date'].dt.year
    min_year = tnr_data['year'].min()
    max_year = tnr_data['year'].max()

    # Create year selector with dropdown including "All Time" option
    years_list = ["All Time"] + list(range(int(max_year), int(min_year) - 1, -1))  # Descending order with All Time first
    selected_period = st.selectbox(
        "Select Time Period",
     years_list,
        index=0  # Default to "All Time"
    )

# Filter data based on selection
    if selected_period == "All Time":
        filtered_data = tnr_data.copy()
        period_title = "All Time"
        comparison_year = max_year  # Use the most recent year for comparison
    else:
        filtered_data = tnr_data[tnr_data['year'] == selected_period]
        period_title = str(selected_period)
        comparison_year = selected_period

# Get top 10 neighborhoods for selected period
    period_tnr_by_postcode = filtered_data.groupby('postcode').size()
    period_top_10_postcodes = period_tnr_by_postcode.nlargest(10).sort_values(ascending=False)

# Create the visualization
    fig, ax = plt.subplots(figsize=(10, 6))

# Create horizontal bar plot with blue color palette
    sns.barplot(x=period_top_10_postcodes.values, 
            y=period_top_10_postcodes.index, 
            palette=sns.color_palette('mako')[::-1], 
            ax=ax)

# Customize the plot
    ax.set_title(f'Top 10 Neighborhoods by TNR Count ({period_title})', pad=20)
    ax.set_xlabel('Number of TNR Procedures')
    ax.set_ylabel('Postcode')

# Add value labels on the bars
    for i, v in enumerate(period_top_10_postcodes.values):
        ax.text(v, i, f' {int(v)}', va='center')

    plt.tight_layout()
    st.pyplot(fig)

# Calculate statistics
    total_period_procedures = len(filtered_data)
    period_unique_cats = len(filtered_data['microchip'].unique())

# Display top neighborhood safely (check if we have data first)
    top_neighborhood = period_top_10_postcodes.index[0] if len(period_top_10_postcodes) > 0 else "N/A"
    top_count = int(period_top_10_postcodes.values[0]) if len(period_top_10_postcodes) > 0 else 0

# Create three columns for metrics
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
        f"Total TNR Procedures ({period_title})",
        f"{total_period_procedures:,}"
    )

    with col2:
        st.metric(
        f"Unique Cats Altered ({period_title})",
        f"{period_unique_cats:,}"
    )

    with col3:
        st.metric(
        "Top Neighborhood",
        f"{top_neighborhood}",
        f"{top_count} procedures"
    )

# Add year-over-year comparison ONLY if a specific year is selected (not All Time)
    if selected_period != "All Time" and selected_period > min_year:
        st.subheader("Year-over-Year Comparison") 
        
        # Get previous year data
        prev_year_data = tnr_data[tnr_data['year'] == (selected_period - 1)]
        prev_year_tnr = prev_year_data.groupby('postcode').size()
        
        # Calculate changes
        yoy_change = total_period_procedures - len(prev_year_data)
        yoy_percent = (yoy_change / len(prev_year_data) * 100) if len(prev_year_data) > 0 else 0
        
        st.write(f"Change from {selected_period-1}: {yoy_change:+,} procedures ({yoy_percent:+.1f}%)")
        
        # Show neighborhood changes
        prev_top_10 = set(prev_year_tnr.nlargest(10).index)
        current_top_10 = set(period_top_10_postcodes.index)
        
        new_to_top_10 = current_top_10 - prev_top_10
        if new_to_top_10:
            st.write("📈 **New to Top 10 this year:**")
            for postcode in new_to_top_10:
                st.write(f"- Postcode {postcode}: {period_tnr_by_postcode[postcode]} TNR procedures")

    # Data Download Section
    st.subheader("Download Data")
    if st.button("Prepare TNR Data for Download"):
        csv = tnr_data.to_csv(index=False)
        st.download_button(
            label="Download TNR Data as CSV",
            data=csv,
        )


def calculate_age_at_tnr(appointments_df, cats_df):
    # Merge appointments with cats data on microchip
    merged_df = pd.merge(appointments_df, cats_df, on='microchip', how='left')
    
    # Convert dates to datetime
    merged_df['date'] = pd.to_datetime(merged_df['date'], utc=True)
    merged_df['last_updated'] = pd.to_datetime(merged_df['last_updated'], utc=True)
    
    # Calculate age at last_updated in months
    merged_df['age_at_last_update_months'] = (merged_df['age_years'].fillna(0) * 12) + merged_df['age_months'].fillna(0)
    
    # Calculate the difference in months between last_updated and TNR date
    merged_df['months_since_last_update'] = (merged_df['date'] - merged_df['last_updated']).dt.days // 30
    
    # Calculate age at TNR
    merged_df['age_at_tnr_months'] = merged_df['age_at_last_update_months'] + merged_df['months_since_last_update']
    
    # Get unique cats (using first TNR appointment for each cat)
    unique_cats = merged_df.sort_values('date').groupby('microchip').first()
    
    # Calculate average age in months
    avg_months = unique_cats['age_at_tnr_months'].mean()
    
    # Convert back to years and months
    avg_years = int(avg_months // 12)
    avg_remaining_months = round(avg_months % 12, 1)
    
    return avg_years, avg_remaining_months

# Calculate age at TNR for the interactive graph
def prepare_age_distribution_data(tnr_data):
    df = tnr_data.copy()
    
    # Ensure date columns are datetime
    df['date'] = pd.to_datetime(df['date'])
    df['last_updated'] = pd.to_datetime(df['last_updated'])
    
    # Calculate current age in months
    df['current_age_months'] = (df['age_years'].fillna(0) * 12) + df['age_months'].fillna(0)
    
    # Calculate how many months have passed since TNR
    df['months_since_tnr'] = (df['last_updated'] - df['date']).dt.days / 30.44
    
    # The age at TNR is the current age minus months since TNR
    df['age_at_tnr_months'] = df['current_age_months'] - df['months_since_tnr']
    
    # Filter out any negative ages or other anomalies
    df = df[df['age_at_tnr_months'] >= 0]
    
    # Round to nearest month for binning
    df['age_at_tnr_months'] = df['age_at_tnr_months'].round().astype(int)
    
    # For each unique cat, use the earliest TNR record
    df_unique = df.sort_values('date').groupby('microchip').first().reset_index()
    
    return df_unique

if __name__ == "__main__":
    main() 