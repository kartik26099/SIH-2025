import pandas as pd 
import folium
from folium.plugins import HeatMap
import re
import numpy as np

def extract_year_from_date(date_str):
    """
    Extract year from date string in DD-MM-YY format
    """
    try:
        if isinstance(date_str, str) and '-' in date_str:
            parts = date_str.split('-')
            if len(parts) >= 3:
                year_part = parts[2]
                # Convert 2-digit year to 4-digit year
                if len(year_part) == 2:
                    year = int(year_part)
                    if year >= 0 and year <= 30:  # 00-30 means 2000-2030
                        return 2000 + year
                    else:  # 31-99 means 1931-1999
                        return 1900 + year
                elif len(year_part) == 4:
                    return int(year_part)
        return None
    except:
        return None

def create_2024_heatmap_improved():
    """
    Create improved heatmap for 2024 data with proper DTWL-based coloring
    """
    try:
        print("🚀 Starting improved 2024 groundwater heatmap creation...")
        
        # Load the data
        print("📖 Loading data from FINAL EXCEL OF Ground water.xlsx...")
        df = pd.read_excel("FINAL EXCEL OF Ground water.xlsx")
        
        print(f"📊 Total rows loaded: {len(df)}")
        print(f"📋 Columns: {list(df.columns)}")
        
        # Filter for 2024 data only
        print("🔍 Filtering for 2024 data...")
        df['Year'] = df['Date'].apply(extract_year_from_date)
        
        # Show year distribution
        year_counts = df['Year'].value_counts().sort_index()
        print("📅 Year distribution:")
        for year, count in year_counts.items():
            if pd.notna(year):
                print(f"   {int(year)}: {count} records")
        
        # Filter for 2024
        df_2024 = df[df['Year'] == 2024].copy()
        print(f"✅ 2024 data: {len(df_2024)} records")
        
        if len(df_2024) == 0:
            print("❌ No 2024 data found!")
            return False
        
        # Remove rows with missing coordinates
        df_2024_clean = df_2024.dropna(subset=['LATITUDE', 'LONGITUDE', 'DTWL'])
        print(f"🧹 After removing missing coordinates: {len(df_2024_clean)} records")
        
        if len(df_2024_clean) == 0:
            print("❌ No valid coordinate data found for 2024!")
            return False
        
        # Analyze DTWL values
        dtwl_values = df_2024_clean['DTWL'].values
        print(f"\n📊 DTWL Statistics:")
        print(f"   Min DTWL: {np.min(dtwl_values):.2f} m")
        print(f"   Max DTWL: {np.max(dtwl_values):.2f} m")
        print(f"   Mean DTWL: {np.mean(dtwl_values):.2f} m")
        print(f"   Median DTWL: {np.median(dtwl_values):.2f} m")
        
        # Define DTWL categories
        # Low DTWL (good): 0-2m (green/blue)
        # Medium DTWL (moderate): 2-5m (yellow)
        # High DTWL (below standard): >5m (red)
        low_threshold = 2.0
        high_threshold = 5.0
        
        low_dtwl = df_2024_clean[df_2024_clean['DTWL'] <= low_threshold]
        medium_dtwl = df_2024_clean[(df_2024_clean['DTWL'] > low_threshold) & (df_2024_clean['DTWL'] <= high_threshold)]
        high_dtwl = df_2024_clean[df_2024_clean['DTWL'] > high_threshold]
        
        print(f"\n📊 DTWL Categories:")
        print(f"   Good (0-2m): {len(low_dtwl)} records")
        print(f"   Moderate (2-5m): {len(medium_dtwl)} records")
        print(f"   Below Standard (>5m): {len(high_dtwl)} records")
        
        # Show sample data
        print("\n📋 Sample 2024 data:")
        print(df_2024_clean[['STATE_UT', 'DISTRICT', 'VILLAGE', 'LATITUDE', 'LONGITUDE', 'Date', 'DTWL']].head())
        
        # Create map centered on India
        print("🗺️ Creating map...")
        m = folium.Map(location=[20.5937, 78.9629], zoom_start=5, tiles='OpenStreetMap')
        
        # Add markers for different DTWL categories
        print("📍 Adding categorized markers...")
        
        # Good DTWL (Green markers)
        for index, row in low_dtwl.iterrows():
            folium.CircleMarker(
                location=[row['LATITUDE'], row['LONGITUDE']],
                radius=8,
                popup=f"""
                <b>State:</b> {row['STATE_UT']}<br>
                <b>District:</b> {row['DISTRICT']}<br>
                <b>Village:</b> {row['VILLAGE']}<br>
                <b>Date:</b> {row['Date']}<br>
                <b>DTWL:</b> {row['DTWL']} m<br>
                <b>Status:</b> <span style="color: green;">GOOD</span>
                """,
                color='darkgreen',
                fill=True,
                fillColor='green',
                fillOpacity=0.8
            ).add_to(m)
        
        # Moderate DTWL (Yellow markers)
        for index, row in medium_dtwl.iterrows():
            folium.CircleMarker(
                location=[row['LATITUDE'], row['LONGITUDE']],
                radius=8,
                popup=f"""
                <b>State:</b> {row['STATE_UT']}<br>
                <b>District:</b> {row['DISTRICT']}<br>
                <b>Village:</b> {row['VILLAGE']}<br>
                <b>Date:</b> {row['Date']}<br>
                <b>DTWL:</b> {row['DTWL']} m<br>
                <b>Status:</b> <span style="color: orange;">MODERATE</span>
                """,
                color='orange',
                fill=True,
                fillColor='yellow',
                fillOpacity=0.8
            ).add_to(m)
        
        # High DTWL (Red markers)
        for index, row in high_dtwl.iterrows():
            folium.CircleMarker(
                location=[row['LATITUDE'], row['LONGITUDE']],
                radius=8,
                popup=f"""
                <b>State:</b> {row['STATE_UT']}<br>
                <b>District:</b> {row['DISTRICT']}<br>
                <b>Village:</b> {row['VILLAGE']}<br>
                <b>Date:</b> {row['Date']}<br>
                <b>DTWL:</b> {row['DTWL']} m<br>
                <b>Status:</b> <span style="color: red;">BELOW STANDARD</span>
                """,
                color='darkred',
                fill=True,
                fillColor='red',
                fillOpacity=0.8
            ).add_to(m)
        
        # Add legend
        legend_html = '''
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 200px; height: 120px; 
                    background-color: white; border:2px solid grey; z-index:9999; 
                    font-size:14px; padding: 10px">
        <p><b>Groundwater Status (2024)</b></p>
        <p><i class="fa fa-circle" style="color:green"></i> Good (0-2m)</p>
        <p><i class="fa fa-circle" style="color:yellow"></i> Moderate (2-5m)</p>
        <p><i class="fa fa-circle" style="color:red"></i> Below Standard (>5m)</p>
        </div>
        '''
        m.get_root().html.add_child(folium.Element(legend_html))
        
        # Add title
        title_html = '''
        <h3 align="center" style="font-size:20px"><b>India Groundwater Level Map - 2024</b></h3>
        <p align="center" style="font-size:14px">Depth to Water Level (DTWL) - Color coded by groundwater status</p>
        '''
        m.get_root().html.add_child(folium.Element(title_html))
        
        # Save map
        output_file = "india_groundwater_heatmap_2024_improved.html"
        print(f"💾 Saving map to {output_file}...")
        m.save(output_file)
        
        print("\n" + "="*60)
        print("🎉 IMPROVED 2024 GROUNDWATER MAP CREATED SUCCESSFULLY!")
        print(f"📊 Total 2024 records: {len(df_2024)}")
        print(f"🟢 Good (0-2m): {len(low_dtwl)} locations")
        print(f"🟡 Moderate (2-5m): {len(medium_dtwl)} locations")
        print(f"🔴 Below Standard (>5m): {len(high_dtwl)} locations")
        print(f"🗺️ Map saved as: {output_file}")
        print("="*60)
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating improved 2024 heatmap: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Starting Improved 2024 Groundwater Map Creation...")
    print("-" * 60)
    
    if create_2024_heatmap_improved():
        print("\n✅ SUCCESS! Your improved 2024 groundwater map is ready!")
        print("🌐 Open 'india_groundwater_heatmap_2024_improved.html' in your browser to view the map.")
    else:
        print("\n❌ Map creation failed!")


