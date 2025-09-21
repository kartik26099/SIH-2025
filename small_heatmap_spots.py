import pandas as pd 
import folium
from folium.plugins import HeatMap
import re
import numpy as np
import os
from supabase import create_client, Client
from dotenv import load_dotenv

def extract_year_from_date(date_str):
    """
    Extract year from date string in DD-MM-YY format
    """
    try:
        if isinstance(date_str, str) and '-' in date_str:
            parts = date_str.split('-')
            if len(parts) >= 3:
                year_part = parts[2]
                if len(year_part) == 2:
                    year = int(year_part)
                    if year >= 0 and year <= 30:
                        return 2000 + year
                    else:
                        return 1900 + year
                elif len(year_part) == 4:
                    return int(year_part)
        return None
    except:
        return None

def create_small_heatmap_spots():
    """
    Create heatmap with small, varied colored spots using Supabase data
    """
    try:
        print("🚀 Creating Small Heatmap Spots Map from Supabase...")
        
        # Load environment variables
        load_dotenv('supabase_config.env')
        
        # Initialize Supabase client
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')
        supabase_table = os.getenv('SUPABASE_TABLE_NAME', 'maharashtra_groundwater_data')
        
        if not supabase_url or not supabase_key:
            print("❌ Supabase credentials not found in supabase_config.env")
            return False
        
        print("🔗 Connecting to Supabase...")
        supabase: Client = create_client(supabase_url, supabase_key)
        
        # Fetch data from Supabase
        print("📖 Fetching data from Supabase...")
        result = supabase.table(supabase_table).select("*").execute()
        
        if not result.data:
            print("❌ No data found in Supabase table!")
            return False
        
        # Convert to DataFrame
        df = pd.DataFrame(result.data)
        print(f"📊 Total rows loaded from Supabase: {len(df)}")
        
        # Remove rows with missing coordinates or well_depth
        df_clean = df.dropna(subset=['latitude', 'longitude', 'well_depth'])
        print(f"🧹 After removing missing coordinates: {len(df_clean)} records")
        
        if len(df_clean) == 0:
            print("❌ No valid data found!")
            return False
        
        # Create map
        print("🗺️ Creating map with small heatmap spots...")
        m = folium.Map(location=[20.5937, 78.9629], zoom_start=5, tiles='OpenStreetMap')
        
        # Create heatmap data with proper intensity scaling
        print("🔥 Creating heatmap data with varied colors...")
        
        # Normalize well_depth values for better color distribution
        # Lower well_depth = higher intensity (better groundwater)
        # Higher well_depth = lower intensity (worse groundwater)
        max_depth = df_clean['well_depth'].max()
        min_depth = df_clean['well_depth'].min()
        
        # Create intensity values (inverted so lower well_depth = higher intensity)
        df_clean['intensity'] = 1 - ((df_clean['well_depth'] - min_depth) / (max_depth - min_depth))
        
        # Create heatmap data
        heat_data = [[row['latitude'], row['longitude'], row['intensity']] for index, row in df_clean.iterrows()]
        
        # Add heatmap with small radius and proper gradient
        HeatMap(
            heat_data,
            radius=8,  # Small radius for small spots
            gradient={
                0.0: 'blue',      # Low intensity = blue (high DTWL = bad)
                0.2: 'cyan',      # 
                0.4: 'green',     # Medium intensity = green
                0.6: 'yellow',    # 
                0.8: 'orange',    # High intensity = orange
                1.0: 'red'        # Very high intensity = red (low DTWL = good)
            },
            min_opacity=0.3,
            max_zoom=18,
            blur=5,  # Small blur for smooth spots
            name='Groundwater Heatmap'
        ).add_to(m)
        
        # Add individual small markers for better visibility
        print("📍 Adding small individual markers...")
        
        # Create feature groups for different well_depth categories
        good_group = folium.FeatureGroup(name='Good Groundwater (0-5m)', show=True)
        moderate_group = folium.FeatureGroup(name='Moderate Groundwater (5-15m)', show=True)
        poor_group = folium.FeatureGroup(name='Poor Groundwater (15-30m)', show=True)
        critical_group = folium.FeatureGroup(name='Critical Groundwater (>30m)', show=True)
        
        # Add individual points with small markers
        for _, row in df_clean.iterrows():
            lat = row['latitude']
            lon = row['longitude']
            well_depth = row['well_depth']
            
            popup_html = f"""
            <div style="width: 150px;">
                <h5 style="margin: 0;">{row['station_name']}, {row['district']}</h5>
                <p style="margin: 2px 0;"><b>State:</b> {row['state']}</p>
                <p style="margin: 2px 0;"><b>Well Depth:</b> {well_depth} m</p>
                <p style="margin: 2px 0;"><b>Station Code:</b> {row['station_code']}</p>
                <p style="margin: 2px 0;"><b>Status:</b> {get_status_text(well_depth)}</p>
            </div>
            """
            
            # Small markers with different colors
            if well_depth <= 5.0:
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=1.5,  # Very small radius
                    popup=folium.Popup(popup_html, max_width=200),
                    color='darkgreen',
                    fill=True,
                    fillColor='green',
                    fillOpacity=0.7,
                    weight=0.5
                ).add_to(good_group)
            elif well_depth <= 15.0:
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=1.5,  # Very small radius
                    popup=folium.Popup(popup_html, max_width=200),
                    color='darkblue',
                    fill=True,
                    fillColor='blue',
                    fillOpacity=0.7,
                    weight=0.5
                ).add_to(moderate_group)
            elif well_depth <= 30.0:
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=1.5,  # Very small radius
                    popup=folium.Popup(popup_html, max_width=200),
                    color='orange',
                    fill=True,
                    fillColor='yellow',
                    fillOpacity=0.7,
                    weight=0.5
                ).add_to(poor_group)
            else:
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=1.5,  # Very small radius
                    popup=folium.Popup(popup_html, max_width=200),
                    color='darkred',
                    fill=True,
                    fillColor='red',
                    fillOpacity=0.7,
                    weight=0.5
                ).add_to(critical_group)
        
        # Add feature groups to map
        good_group.add_to(m)
        moderate_group.add_to(m)
        poor_group.add_to(m)
        critical_group.add_to(m)
        
        # Add layer control
        folium.LayerControl().add_to(m)
        
        # Add legend
        legend_html = '''
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 300px; height: 200px; 
                    background-color: white; border:2px solid grey; z-index:9999; 
                    font-size:14px; padding: 15px; border-radius: 5px;">
        <h4 style="margin: 0 0 10px 0; text-align: center;">Maharashtra Groundwater Heatmap</h4>
        <p style="margin: 5px 0;"><b>Small Heatmap Spots:</b></p>
        <p style="margin: 2px 0;"><i class="fa fa-circle" style="color:green"></i> Good (0-5m)</p>
        <p style="margin: 2px 0;"><i class="fa fa-circle" style="color:blue"></i> Moderate (5-15m)</p>
        <p style="margin: 2px 0;"><i class="fa fa-circle" style="color:yellow"></i> Poor (15-30m)</p>
        <p style="margin: 2px 0;"><i class="fa fa-circle" style="color:red"></i> Critical (>30m)</p>
        <hr style="margin: 10px 0;">
        <p style="margin: 5px 0;"><small><b>Heatmap Colors:</b> Blue (bad) → Green → Yellow → Orange → Red (good)</small></p>
        </div>
        '''
        m.get_root().html.add_child(folium.Element(legend_html))
        
        # Add title
        title_html = '''
        <h2 align="center" style="font-size:24px; margin: 10px 0;"><b>Maharashtra Groundwater Level Map</b></h2>
        <p align="center" style="font-size:16px; margin: 5px 0;">Small Heatmap Spots with Color Variation (Data from Supabase)</p>
        '''
        m.get_root().html.add_child(folium.Element(title_html))
        
        # Save map
        output_file = "maharashtra_supabase_heatmap_spots.html"
        print(f"💾 Saving small heatmap spots map to {output_file}...")
        m.save(output_file)
        
        print("\n" + "="*60)
        print("🎉 SUPABASE HEATMAP SPOTS MAP CREATED SUCCESSFULLY!")
        print(f"📊 Total records from Supabase: {len(df_clean)}")
        print(f"🗺️ Map saved as: {output_file}")
        print("✅ Small heatmap spots with color variation!")
        print("✅ Data fetched from Supabase database!")
        print("✅ Using latitude, longitude, and well_depth!")
        print("="*60)
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating small heatmap spots map: {e}")
        return False

def get_status_text(well_depth):
    """
    Get status text based on well_depth value
    """
    if well_depth <= 5.0:
        return "Good"
    elif well_depth <= 15.0:
        return "Moderate"
    elif well_depth <= 30.0:
        return "Poor"
    else:
        return "Critical"

if __name__ == "__main__":
    print("🚀 Creating Small Heatmap Spots Map from Supabase...")
    print("-" * 60)
    
    if create_small_heatmap_spots():
        print("\n✅ SUCCESS! Your Supabase heatmap spots map is ready!")
        print("🌐 Open 'maharashtra_supabase_heatmap_spots.html' in your browser to view!")
        print("💡 Small heatmap spots with data from Supabase database!")
    else:
        print("\n❌ Map creation failed!")


