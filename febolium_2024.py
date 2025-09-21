import pandas as pd 
import folium
from folium.plugins import HeatMap
import re

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

def create_2024_heatmap():
    """
    Create heatmap for 2024 data only
    """
    try:
        print("🚀 Starting 2024 groundwater heatmap creation...")
        
        # Load the properly formatted data
        print("📖 Loading data from pre-monsoon_2014-2024_complete.xlsx...")
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
        
        # Show sample data
        print("\n📋 Sample 2024 data:")
        print(df_2024_clean[['STATE_UT', 'DISTRICT', 'VILLAGE', 'LATITUDE', 'LONGITUDE', 'Date', 'DTWL']].head())
        
        # Create map centered on India
        print("🗺️ Creating map...")
        m = folium.Map(location=[20.5937, 78.9629], zoom_start=5, tiles='OpenStreetMap')
        
        # Prepare heatmap data
        print("🔥 Preparing heatmap data...")
        heat_data = []
        
        for index, row in df_2024_clean.iterrows():
            lat = row['LATITUDE']
            lon = row['LONGITUDE']
            dtwl = row['DTWL']
            
            # Validate coordinates
            if pd.notna(lat) and pd.notna(lon) and pd.notna(dtwl):
                # Check if coordinates are within reasonable bounds for India
                if 6.0 <= lat <= 37.0 and 68.0 <= lon <= 97.0:
                    heat_data.append([lat, lon, dtwl])
        
        print(f"📍 Valid heatmap points: {len(heat_data)}")
        
        if len(heat_data) == 0:
            print("❌ No valid heatmap data points found!")
            return False
        
        # Add heatmap layer
        print("🔥 Adding heatmap layer...")
        HeatMap(heat_data, 
                radius=15, 
                blur=10, 
                max_zoom=18,
                gradient={0.4: 'blue', 0.6: 'cyan', 0.7: 'lime', 0.8: 'yellow', 1.0: 'red'}
               ).add_to(m)
        
        # Add markers for some sample points
        print("📍 Adding sample markers...")
        sample_data = df_2024_clean.head(10)  # First 10 points
        
        for index, row in sample_data.iterrows():
            folium.CircleMarker(
                location=[row['LATITUDE'], row['LONGITUDE']],
                radius=5,
                popup=f"""
                <b>State:</b> {row['STATE_UT']}<br>
                <b>District:</b> {row['DISTRICT']}<br>
                <b>Village:</b> {row['VILLAGE']}<br>
                <b>Date:</b> {row['Date']}<br>
                <b>DTWL:</b> {row['DTWL']} m
                """,
                color='red',
                fill=True,
                fillColor='red'
            ).add_to(m)
        
        # Add title
        title_html = '''
        <h3 align="center" style="font-size:20px"><b>India Groundwater Level Heatmap - 2024</b></h3>
        <p align="center" style="font-size:14px">Depth to Water Level (DTWL) in meters below ground level</p>
        '''
        m.get_root().html.add_child(folium.Element(title_html))
        
        # Save map
        output_file = "india_groundwater_heatmap_2024.html"
        print(f"💾 Saving map to {output_file}...")
        m.save(output_file)
        
        print("\n" + "="*60)
        print("🎉 2024 GROUNDWATER HEATMAP CREATED SUCCESSFULLY!")
        print(f"📊 Total 2024 records: {len(df_2024)}")
        print(f"📍 Valid heatmap points: {len(heat_data)}")
        print(f"🗺️ Map saved as: {output_file}")
        print("="*60)
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating 2024 heatmap: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Starting 2024 Groundwater Heatmap Creation...")
    print("-" * 60)
    
    if create_2024_heatmap():
        print("\n✅ SUCCESS! Your 2024 groundwater heatmap is ready!")
        print("🌐 Open 'india_groundwater_heatmap_2024.html' in your browser to view the map.")
    else:
        print("\n❌ Heatmap creation failed!")
