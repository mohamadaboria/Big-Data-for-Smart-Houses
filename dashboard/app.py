"""
Smart House Analytics Dashboard
Real-time web application for visualizing IoT telemetry data
Compatible with Python 3.11+ with proper historical data handling
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import sys
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional

# Streamlit autorefresh import
try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    st.error("streamlit-autorefresh not installed. Please run: pip install streamlit-autorefresh")
    st.stop()

# Add parent directory to path for config import
sys.path.append(str(Path(__file__).parent.parent))
from config.config import *

# Configure logging
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# Streamlit page configuration
st.set_page_config(
    page_title="🏠 Smart House Analytics",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Color palette for consistent styling
COLOR_PALETTE = {
    'thermostat': '#FF6B6B',
    'smart_bulb': '#4ECDC4',
    'smart_plug': '#45B7D1',
    'security_camera': '#96CEB4',
    'motion_sensor': '#FFEAA7'
}

def get_all_data_files(data_type: str, days_back: int = 7) -> List[str]:
    """Get ALL data files for the specified number of days back"""
    try:
        # Path mapping for different data types
        path_mapping = {
            "energy_usage": SILVER_ENERGY_DIR,
            "daily_energy_consumption": GOLD_DAILY_ENERGY_DIR,
            "device_health_metrics": GOLD_DEVICE_HEALTH_DIR,
            "daily_business_summary": GOLD_BUSINESS_SUMMARY_DIR
        }
        
        search_dir = path_mapping.get(data_type)
        if not search_dir or not search_dir.exists():
            return []
        
        files = []
        end_date = datetime.now().date()
        
        # For date-partitioned data (business summary)
        if data_type == "daily_business_summary":
            for days in range(days_back):
                check_date = end_date - timedelta(days=days)
                date_str = check_date.strftime('%Y-%m-%d')
                date_dir = search_dir / date_str
                if date_dir.exists():
                    for file_path in date_dir.glob('*.parquet'):
                        files.append(str(file_path))
        
        # For energy usage (date-partitioned in Silver layer)
        elif data_type == "energy_usage":
            for days in range(days_back):
                check_date = end_date - timedelta(days=days)
                date_str = check_date.strftime('%Y-%m-%d')
                date_dir = search_dir / date_str
                if date_dir.exists():
                    for file_path in date_dir.glob('*.parquet'):
                        files.append(str(file_path))
        
        # For non-partitioned data (device health, daily energy consumption)
        else:
            # Get files from the last N days based on modification time
            cutoff_time = datetime.now() - timedelta(days=days_back)
            for file_path in search_dir.rglob('*.parquet'):
                try:
                    file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if file_mtime >= cutoff_time:
                        files.append(str(file_path))
                except Exception as e:
                    logger.warning(f"Error checking file time {file_path}: {e}")
        
        # Sort by modification time, newest first
        files.sort(key=lambda x: Path(x).stat().st_mtime, reverse=True)
        
        logger.info(f"Found {len(files)} files for {data_type} (last {days_back} days)")
        return files
        
    except Exception as e:
        logger.error(f"Error getting data files for {data_type}: {e}")
        return []

def get_recent_data_files(data_type: str, hours_back: int = 2) -> List[str]:
    """Get only recent data files for real-time updates"""
    try:
        path_mapping = {
            "energy_usage": SILVER_ENERGY_DIR,
            "daily_energy_consumption": GOLD_DAILY_ENERGY_DIR,
            "device_health_metrics": GOLD_DEVICE_HEALTH_DIR,
            "daily_business_summary": GOLD_BUSINESS_SUMMARY_DIR
        }
        
        search_dir = path_mapping.get(data_type)
        if not search_dir or not search_dir.exists():
            return []
        
        files = []
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        
        for file_path in search_dir.rglob('*.parquet'):
            try:
                file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                if file_mtime >= cutoff_time:
                    files.append(str(file_path))
            except Exception as e:
                logger.warning(f"Error checking file time {file_path}: {e}")
        
        files.sort(key=lambda x: Path(x).stat().st_mtime, reverse=True)
        return files
        
    except Exception as e:
        logger.error(f"Error getting recent data files for {data_type}: {e}")
        return []

@st.cache_data(ttl=300)  # Cache for 5 minutes for historical data
def load_historical_data(data_type: str, days_back: int = 7) -> pd.DataFrame:
    """Load historical data with longer cache time"""
    try:
        files = get_all_data_files(data_type, days_back)
        
        if not files:
            logger.warning(f"No historical files found for {data_type}")
            return pd.DataFrame()
        
        # Combine multiple parquet files
        dfs = []
        for file in files:
            try:
                df = pd.read_parquet(file)
                dfs.append(df)
            except Exception as e:
                logger.warning(f"Error loading file {file}: {e}")
        
        if not dfs:
            return pd.DataFrame()
        
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Convert timestamp columns if they exist
        timestamp_cols = ['timestamp', 'created_at', 'date']
        for col in timestamp_cols:
            if col in combined_df.columns:
                if col == 'date':
                    combined_df[col] = pd.to_datetime(combined_df[col]).dt.date
                else:
                    combined_df[col] = pd.to_datetime(combined_df[col], utc=True)
        
        # Remove duplicates
        if 'timestamp' in combined_df.columns and 'device_id' in combined_df.columns:
            combined_df = combined_df.drop_duplicates(subset=['device_id', 'timestamp'])
        elif 'date' in combined_df.columns and 'device_id' in combined_df.columns:
            combined_df = combined_df.drop_duplicates(subset=['device_id', 'date'])
        
        logger.info(f"Loaded {len(combined_df)} historical records from {data_type}")
        return combined_df
        
    except Exception as e:
        logger.error(f"Error loading historical data for {data_type}: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=30)  # Cache for 30 seconds for recent data
def load_recent_data(data_type: str, hours_back: int = 2) -> pd.DataFrame:
    """Load recent data with short cache time for real-time updates"""
    try:
        files = get_recent_data_files(data_type, hours_back)
        
        if not files:
            return pd.DataFrame()
        
        # Combine recent files
        dfs = []
        for file in files:
            try:
                df = pd.read_parquet(file)
                dfs.append(df)
            except Exception as e:
                logger.warning(f"Error loading recent file {file}: {e}")
        
        if not dfs:
            return pd.DataFrame()
        
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Convert timestamp columns
        timestamp_cols = ['timestamp', 'created_at', 'date']
        for col in timestamp_cols:
            if col in combined_df.columns:
                if col == 'date':
                    combined_df[col] = pd.to_datetime(combined_df[col]).dt.date
                else:
                    combined_df[col] = pd.to_datetime(combined_df[col], utc=True)
        
        logger.info(f"Loaded {len(combined_df)} recent records from {data_type}")
        return combined_df
        
    except Exception as e:
        logger.error(f"Error loading recent data for {data_type}: {e}")
        return pd.DataFrame()

def combine_historical_and_recent_data(historical_df: pd.DataFrame, recent_df: pd.DataFrame) -> pd.DataFrame:
    """Combine historical and recent data, removing duplicates"""
    if historical_df.empty:
        return recent_df
    if recent_df.empty:
        return historical_df
    
    try:
        # Combine dataframes
        combined_df = pd.concat([historical_df, recent_df], ignore_index=True)
        
        # Remove duplicates based on available columns
        if 'timestamp' in combined_df.columns and 'device_id' in combined_df.columns:
            combined_df = combined_df.drop_duplicates(subset=['device_id', 'timestamp'])
        elif 'date' in combined_df.columns and 'device_id' in combined_df.columns:
            combined_df = combined_df.drop_duplicates(subset=['device_id', 'date'])
        elif 'device_id' in combined_df.columns:
            combined_df = combined_df.drop_duplicates(subset=['device_id'])
        
        # Sort by timestamp if available
        if 'timestamp' in combined_df.columns:
            combined_df = combined_df.sort_values('timestamp')
        elif 'date' in combined_df.columns:
            combined_df = combined_df.sort_values('date')
        
        return combined_df
        
    except Exception as e:
        logger.error(f"Error combining historical and recent data: {e}")
        return historical_df

def load_complete_data(data_type: str, days_back: int = 7) -> pd.DataFrame:
    """Load complete data combining historical and recent data"""
    try:
        # Load historical data (cached for 5 minutes)
        historical_data = load_historical_data(data_type, days_back)
        
        # Load recent data (cached for 30 seconds)
        recent_data = load_recent_data(data_type, hours_back=2)
        
        # Combine both datasets
        complete_data = combine_historical_and_recent_data(historical_data, recent_data)
        
        logger.info(f"Complete dataset for {data_type}: {len(complete_data)} records")
        return complete_data
        
    except Exception as e:
        logger.error(f"Error loading complete data for {data_type}: {e}")
        return pd.DataFrame()

def create_kpi_metrics(data: Dict[str, pd.DataFrame]) -> Dict[str, float]:
    """Create KPI metrics for the dashboard with consistent data sources"""
    try:
        kpis = {
            'total_energy_kwh': 0.0,
            'total_cost': 0.0,
            'active_devices': 0,
            'avg_health': 0.0
        }
        
        # Use the SAME data source as the energy chart for consistency
        # Priority: daily_energy_consumption (Gold layer) -> energy_usage (Silver layer)
        
        if not data['daily_energy_consumption'].empty:
            # Use Gold layer data - get TODAY's data for 24h KPI
            daily_energy = data['daily_energy_consumption']
            today = datetime.now().date()
            
            # Filter for today's data
            today_data = daily_energy[daily_energy['date'] == today]
            
            if not today_data.empty:
                # Sum today's energy consumption
                kpis['total_energy_kwh'] = today_data['energy_consumption_wh_sum'].sum() / 1000
                kpis['total_cost'] = today_data['energy_cost_estimate'].sum()
                kpis['active_devices'] = today_data['device_id'].nunique()
                
                logger.info(f"KPI from Gold layer (today): {kpis['total_energy_kwh']:.2f} kWh")
            else:
                # Fallback to last 24 hours from Silver layer
                logger.info("No today's Gold data, falling back to Silver layer for 24h calculation")
                if not data['energy_usage'].empty:
                    energy_data = data['energy_usage']
                    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
                    last_24h = energy_data[energy_data['timestamp'] >= cutoff_time]
                    
                    kpis['total_energy_kwh'] = last_24h['energy_consumption_wh'].sum() / 1000
                    kpis['total_cost'] = kpis['total_energy_kwh'] * ENERGY_RATE_PER_KWH
                    kpis['active_devices'] = last_24h['device_id'].nunique()
                    
                    logger.info(f"KPI from Silver layer (24h): {kpis['total_energy_kwh']:.2f} kWh")
        
        elif not data['energy_usage'].empty:
            # Fallback to Silver layer data - last 24 hours
            energy_data = data['energy_usage']
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
            last_24h = energy_data[energy_data['timestamp'] >= cutoff_time]
            
            kpis['total_energy_kwh'] = last_24h['energy_consumption_wh'].sum() / 1000
            kpis['total_cost'] = kpis['total_energy_kwh'] * ENERGY_RATE_PER_KWH
            kpis['active_devices'] = last_24h['device_id'].nunique()
            
            logger.info(f"KPI from Silver layer only (24h): {kpis['total_energy_kwh']:.2f} kWh")
        
        # Try business summary for comparison/validation
        if not data['daily_business_summary'].empty:
            business_data = data['daily_business_summary']
            latest_summary = business_data.sort_values('date').iloc[-1] if len(business_data) > 0 else None
            
            if latest_summary is not None:
                business_energy = latest_summary.get('total_energy_kwh', 0.0)
                business_cost = latest_summary.get('total_cost_estimate', 0.0)
                
                logger.info(f"Business summary comparison: {business_energy:.2f} kWh, ${business_cost:.2f}")
                
                # Use business summary if KPI values are still zero
                if kpis['total_energy_kwh'] == 0.0:
                    kpis['total_energy_kwh'] = business_energy
                    kpis['total_cost'] = business_cost
                    kpis['active_devices'] = latest_summary.get('active_devices', 0)
        
        # Device health from device health metrics
        if kpis['avg_health'] == 0.0 and not data['device_health_metrics'].empty:
            health_data = data['device_health_metrics']
            kpis['avg_health'] = health_data['health_score'].mean() * 100
        
        return kpis
        
    except Exception as e:
        logger.error(f"Error creating KPI metrics: {e}")
        return {'total_energy_kwh': 0.0, 'total_cost': 0.0, 'active_devices': 0, 'avg_health': 0.0}

def create_energy_usage_chart(data: Dict[str, pd.DataFrame]) -> go.Figure:
    """Create energy usage by device type chart with historical data"""
    try:
        # Use daily energy consumption for aggregated view (SAME as KPI calculation)
        if not data['daily_energy_consumption'].empty:
            # For chart: show total over last 7 days
            energy_by_type = data['daily_energy_consumption'].groupby('device_type')['energy_consumption_wh_sum'].sum().reset_index()
            energy_by_type = energy_by_type.rename(columns={'energy_consumption_wh_sum': 'energy_consumption_wh'})
            energy_by_type['energy_consumption_wh'] = energy_by_type['energy_consumption_wh'] / 1000  # Convert to kWh
            title = 'Total Energy Usage by Device Type (Last 7 Days)'
            
            # Log for debugging
            total_chart_energy = energy_by_type['energy_consumption_wh'].sum()
            logger.info(f"Chart total energy (7 days): {total_chart_energy:.2f} kWh")
            
        else:
            # Fallback to Silver layer data
            if data['energy_usage'].empty:
                return go.Figure().add_annotation(text="No energy data available", 
                                                xref="paper", yref="paper", x=0.5, y=0.5)
            
            energy_by_type = data['energy_usage'].groupby('device_type')['energy_consumption_wh'].sum().reset_index()
            energy_by_type['energy_consumption_wh'] = energy_by_type['energy_consumption_wh'] / 1000  # Convert to kWh
            title = 'Energy Usage by Device Type (All Data)'
        
        # Create bar chart
        fig = px.bar(
            energy_by_type,
            x='device_type',
            y='energy_consumption_wh',
            title=title,
            labels={'energy_consumption_wh': 'Energy (kWh)', 'device_type': 'Device Type'},
            color='device_type',
            color_discrete_map=COLOR_PALETTE
        )
        
        fig.update_layout(
            showlegend=False,
            height=400,
            font=dict(family="Arial, sans-serif"),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)'
        )
        
        return fig
        
    except Exception as e:
        logger.error(f"Error creating energy usage chart: {e}")
        return go.Figure().add_annotation(text="Error loading energy data", 
                                        xref="paper", yref="paper", x=0.5, y=0.5)

def create_energy_trend_chart(data: Dict[str, pd.DataFrame]) -> go.Figure:
    """Create energy usage trend over time"""
    try:
        if data['daily_energy_consumption'].empty:
            return go.Figure().add_annotation(text="No daily energy data available", 
                                            xref="paper", yref="paper", x=0.5, y=0.5)
        
        # Group by date and sum energy consumption
        daily_trend = data['daily_energy_consumption'].groupby('date')['energy_consumption_wh_sum'].sum().reset_index()
        daily_trend['energy_consumption_kwh'] = daily_trend['energy_consumption_wh_sum'] / 1000
        daily_trend = daily_trend.sort_values('date')
        
        # Log today's value for debugging
        today = datetime.now().date()
        today_trend = daily_trend[daily_trend['date'] == today]
        if not today_trend.empty:
            today_energy = today_trend['energy_consumption_kwh'].iloc[0]
            logger.info(f"Today's energy in trend chart: {today_energy:.2f} kWh")
        
        fig = px.line(
            daily_trend,
            x='date',
            y='energy_consumption_kwh',
            title='Daily Energy Consumption Trend',
            labels={'energy_consumption_kwh': 'Energy (kWh)', 'date': 'Date'},
            markers=True
        )
        
        # Add data labels on points
        fig.update_traces(
            text=[f"{energy:.1f}" for energy in daily_trend['energy_consumption_kwh']],
            textposition="top center",
            textfont=dict(size=10)
        )
        
        fig.update_layout(
            height=400,
            font=dict(family="Arial, sans-serif"),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)'
        )
        
        return fig
        
    except Exception as e:
        logger.error(f"Error creating energy trend chart: {e}")
        return go.Figure().add_annotation(text="Error loading trend data", 
                                        xref="paper", yref="paper", x=0.5, y=0.5)

def create_daily_cost_chart(data: Dict[str, pd.DataFrame]) -> go.Figure:
    """Create daily cost trend chart - EXACTLY like energy chart but with green line"""
    try:
        if data['daily_energy_consumption'].empty:
            return go.Figure().add_annotation(text="No daily cost data available", 
                                            xref="paper", yref="paper", x=0.5, y=0.5)
        
        # Use SAME logic as energy trend chart but calculate cost
        daily_cost = data['daily_energy_consumption'].groupby('date')['energy_consumption_wh_sum'].sum().reset_index()
        
        # Calculate cost from energy consumption (same as energy chart logic)
        daily_cost['daily_cost'] = (daily_cost['energy_consumption_wh_sum'] / 1000) * ENERGY_RATE_PER_KWH
        daily_cost = daily_cost.sort_values('date')
        
        fig = px.line(
            daily_cost,
            x='date',
            y='daily_cost',
            title='Daily Energy Cost Trend',
            labels={'daily_cost': 'Cost ($)', 'date': 'Date'},
            markers=True
        )
        
        # Add data labels on points (same as energy chart)
        fig.update_traces(
            text=[f"${cost:.2f}" for cost in daily_cost['daily_cost']],
            textposition="top center",
            textfont=dict(size=10),
            line_color='#28a745',  # Green line
            marker_color='#28a745'  # Green markers
        )
        
        # Same layout as energy chart
        fig.update_layout(
            height=400,
            font=dict(family="Arial, sans-serif"),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)'
        )
        
        return fig
        
    except Exception as e:
        logger.error(f"Error creating daily cost chart: {e}")
        return go.Figure().add_annotation(text="Error loading cost data", 
                                        xref="paper", yref="paper", x=0.5, y=0.5)

def create_device_health_chart(data: Dict[str, pd.DataFrame]) -> go.Figure:
    """Create device health scatter plot"""
    try:
        if data['device_health_metrics'].empty:
            return go.Figure().add_annotation(text="No device health data available", 
                                            xref="paper", yref="paper", x=0.5, y=0.5)
        
        health_data = data['device_health_metrics']
        
        # Create scatter plot
        fig = px.scatter(
            health_data,
            x='health_score',
            y='failure_probability',
            size='total_alerts',
            color='device_type',
            title='Device Health vs Failure Risk',
            labels={
                'health_score': 'Health Score',
                'failure_probability': 'Failure Probability',
                'total_alerts': 'Total Alerts'
            },
            color_discrete_map=COLOR_PALETTE,
            hover_data=['device_id']
        )
        
        fig.update_layout(
            height=400,
            font=dict(family="Arial, sans-serif"),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)'
        )
        
        return fig
        
    except Exception as e:
        logger.error(f"Error creating device health chart: {e}")
        return go.Figure().add_annotation(text="Error loading health data", 
                                        xref="paper", yref="paper", x=0.5, y=0.5)

def create_live_temperature_chart(data: Dict[str, pd.DataFrame]) -> go.Figure:
    """Create live temperature monitoring chart with timezone fix"""
    try:
        if data['energy_usage'].empty:
            return go.Figure().add_annotation(text="No temperature data available", 
                                            xref="paper", yref="paper", x=0.5, y=0.5)
        
        temp_data = data['energy_usage'].copy()
        
        # Check if we have timestamp column and temperature data
        if 'timestamp' not in temp_data.columns or 'temperature' not in temp_data.columns:
            return go.Figure().add_annotation(text="Missing timestamp or temperature data", 
                                            xref="paper", yref="paper", x=0.5, y=0.5)
        
        # Filter for last 2 hours using timezone-aware datetime
        if not temp_data.empty:
            # Use timezone-aware datetime for comparison
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=2)
            temp_data = temp_data[temp_data['timestamp'] >= cutoff_time]
        
        # Get last N records for performance
        temp_data = temp_data.sort_values('timestamp').tail(MAX_LIVE_CHART_RECORDS)
        
        if temp_data.empty:
            return go.Figure().add_annotation(text="No recent temperature data (last 2 hours)", 
                                            xref="paper", yref="paper", x=0.5, y=0.5)
        
        fig = px.line(
            temp_data,
            x='timestamp',
            y='temperature',
            color='device_id',
            title='Live Temperature Monitoring (Last 2 Hours)',
            labels={'temperature': 'Temperature (°C)', 'timestamp': 'Time'}
        )
        
        fig.update_layout(
            height=300,
            font=dict(family="Arial, sans-serif"),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        return fig
        
    except Exception as e:
        logger.error(f"Error creating temperature chart: {e}")
        return go.Figure().add_annotation(text=f"Error: {str(e)}", 
                                        xref="paper", yref="paper", x=0.5, y=0.5)

def create_live_power_chart(data: Dict[str, pd.DataFrame]) -> go.Figure:
    """Create live power usage chart with timezone fix"""
    try:
        if data['energy_usage'].empty:
            return go.Figure().add_annotation(text="No power data available", 
                                            xref="paper", yref="paper", x=0.5, y=0.5)
        
        power_data = data['energy_usage'].copy()
        
        # Check if we have timestamp column and power data
        if 'timestamp' not in power_data.columns or 'power_usage' not in power_data.columns:
            return go.Figure().add_annotation(text="Missing timestamp or power usage data", 
                                            xref="paper", yref="paper", x=0.5, y=0.5)
        
        # Filter for last 2 hours using timezone-aware datetime
        if not power_data.empty:
            # Use timezone-aware datetime for comparison
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=2)
            power_data = power_data[power_data['timestamp'] >= cutoff_time]
        
        # Get last N records for performance
        power_data = power_data.sort_values('timestamp').tail(MAX_LIVE_CHART_RECORDS)
        
        if power_data.empty:
            return go.Figure().add_annotation(text="No recent power data (last 2 hours)", 
                                            xref="paper", yref="paper", x=0.5, y=0.5)
        
        fig = px.line(
            power_data,
            x='timestamp',
            y='power_usage',
            color='device_id',
            title='Live Power Usage (Last 2 Hours)',
            labels={'power_usage': 'Power (Watts)', 'timestamp': 'Time'}
        )
        
        fig.update_layout(
            height=300,
            font=dict(family="Arial, sans-serif"),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        return fig
        
    except Exception as e:
        logger.error(f"Error creating power chart: {e}")
        return go.Figure().add_annotation(text=f"Error: {str(e)}", 
                                        xref="paper", yref="paper", x=0.5, y=0.5)

def create_health_gauge(avg_health: float) -> go.Figure:
    """Create system health gauge"""
    try:
        fig = go.Figure(go.Indicator(
            mode = "gauge+number+delta",
            value = avg_health,
            domain = {'x': [0, 1], 'y': [0, 1]},
            title = {'text': "System Health (%)"},
            delta = {'reference': 90},
            gauge = {
                'axis': {'range': [None, 100]},
                'bar': {'color': "darkblue"},
                'steps': [
                    {'range': [0, 50], 'color': "lightgray"},
                    {'range': [50, 80], 'color': "gray"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 90
                }
            }
        ))
        
        fig.update_layout(height=300, font={'size': 16})
        return fig
        
    except Exception as e:
        logger.error(f"Error creating health gauge: {e}")
        return go.Figure()

DASHBOARD_REFRESH_INTERVAL = 120

def main():
    """Main dashboard function"""
    # Title and header
    st.title("🏠 Smart House Analytics Dashboard")
    st.markdown("Real-time monitoring of IoT devices, energy consumption, and system health")
    
    # Auto-refresh using streamlit-autorefresh
    refresh_count = st_autorefresh(interval=DASHBOARD_REFRESH_INTERVAL * 1000, key="data_refresh")  # 30 seconds
    
    # Load all data with proper historical handling
    with st.spinner("Loading data..."):
        data = {
            'energy_usage': load_complete_data("energy_usage", days_back=7),
            'daily_energy_consumption': load_complete_data("daily_energy_consumption", days_back=7),
            'device_health_metrics': load_complete_data("device_health_metrics", days_back=7),
            'daily_business_summary': load_complete_data("daily_business_summary", days_back=7)
        }
    
    # Sidebar with data status
    with st.sidebar:
        st.header("📊 Data Status")
        
        # Show refresh counter
        st.write(f"**Auto-refresh Count:** {refresh_count}")
        st.write(f"**Last Refresh:** {datetime.now().strftime('%H:%M:%S')}")
        
        # Data summary with historical context
        st.metric("📊 Energy Records (7 days)", len(data['energy_usage']))
        st.metric("⚡ Daily Energy Records", len(data['daily_energy_consumption']))
        st.metric("❤️ Device Health Records", len(data['device_health_metrics']))
        st.metric("📈 Business Summary Records", len(data['daily_business_summary']))
        
        # Data date range
        if not data['energy_usage'].empty and 'timestamp' in data['energy_usage'].columns:
            min_date = data['energy_usage']['timestamp'].min()
            max_date = data['energy_usage']['timestamp'].max()
            st.write(f"**Data Range:**")
            st.write(f"From: {min_date.strftime('%Y-%m-%d %H:%M')}")
            st.write(f"To: {max_date.strftime('%Y-%m-%d %H:%M')}")
        
        # Show today's data availability for debugging
        if not data['daily_energy_consumption'].empty:
            today = datetime.now().date()
            today_data = data['daily_energy_consumption'][data['daily_energy_consumption']['date'] == today]
            st.write(f"**Today's Records:** {len(today_data)}")
            if not today_data.empty:
                today_total = today_data['energy_consumption_wh_sum'].sum() / 1000
                today_cost = (today_total * ENERGY_RATE_PER_KWH)
                st.write(f"**Today's Total:** {today_total:.2f} kWh")
                st.write(f"**Today's Cost:** ${today_cost:.2f}")
        
        # Manual refresh button
        if st.button("🔄 Refresh Now"):
            st.cache_data.clear()
            st.rerun()
    
    # Create KPI metrics
    kpis = create_kpi_metrics(data)
    
    # KPI Row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="⚡ Total Energy (Today)",
            value=f"{kpis['total_energy_kwh']:.2f} kWh",
            delta=None
        )
    
    with col2:
        st.metric(
            label="💰 Total Cost (Today)",
            value=f"${kpis['total_cost']:.2f}",
            delta=None
        )
    
    with col3:
        st.metric(
            label="📱 Active Devices",
            value=int(kpis['active_devices']),
            delta=None
        )
    
    with col4:
        st.metric(
            label="❤️ Average Health",
            value=f"{kpis['avg_health']:.1f}%",
            delta=None
        )
    
    # Main Charts Row - Historical Analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.plotly_chart(create_energy_usage_chart(data), use_container_width=True)
    
    with col2:
        st.plotly_chart(create_energy_trend_chart(data), use_container_width=True)
    
    # Second Charts Row - Cost and Health
    col1, col2 = st.columns(2)
    
    with col1:
        st.plotly_chart(create_daily_cost_chart(data), use_container_width=True)
    
    with col2:
        st.plotly_chart(create_device_health_chart(data), use_container_width=True)
    
    # Third Charts Row - Health Gauge
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.plotly_chart(create_health_gauge(kpis['avg_health']), use_container_width=True)
    
    # Live Monitoring Row - Real-time Data
    st.subheader("🔴 Live Monitoring (Last 2 Hours)")
    col1, col2 = st.columns(2)
    
    with col1:
        st.plotly_chart(create_live_temperature_chart(data), use_container_width=True)
    
    with col2:
        st.plotly_chart(create_live_power_chart(data), use_container_width=True)
    
    # Debug Information (expandable)
    with st.expander("🔧 Debug Information"):
        st.write("**Working Directory:**", str(Path.cwd()))
        st.write("**Data Directory Exists:**", DATA_DIR.exists())
        st.write("**Streamlit Version:**", st.__version__)
        
        # Data summary with date ranges
        st.write("**Data Summary:**")
        for key, df in data.items():
            if not df.empty:
                if 'timestamp' in df.columns:
                    date_range = f"{df['timestamp'].min()} to {df['timestamp'].max()}"
                elif 'date' in df.columns:
                    date_range = f"{df['date'].min()} to {df['date'].max()}"
                else:
                    date_range = "No date info"
                st.write(f"  - {key}: {len(df)} records ({date_range})")
            else:
                st.write(f"  - {key}: 0 records")
        
        # Show sample data for debugging
        if not data['daily_energy_consumption'].empty:
            st.write("**Sample Daily Energy Data:**")
            sample_data = data['daily_energy_consumption'].head(3)
            st.dataframe(sample_data[['device_id', 'date', 'energy_consumption_wh_sum']])
        
        # Show KPI calculation details
        st.write("**KPI Calculation Details:**")
        st.write(f"- Energy KPI: {kpis['total_energy_kwh']:.2f} kWh")
        st.write(f"- Cost KPI: ${kpis['total_cost']:.2f}")
        st.write(f"- Active Devices: {kpis['active_devices']}")
        st.write(f"- Energy Rate: ${ENERGY_RATE_PER_KWH:.3f}/kWh")
    
    # Footer
    st.markdown("---")
    st.markdown(f"*Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Auto-refresh: {DASHBOARD_REFRESH_INTERVAL}s | Historical data: 7 days*")

if __name__ == "__main__":
    main()

