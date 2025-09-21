import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import json

def fetch_all_maharashtra_groundwater_data():
    """
    Fetch real-time groundwater data for ALL Maharashtra districts from the API
    """
    print("🚀 Fetching ALL Maharashtra Groundwater Data...")
    
    # Read the unique states and districts file
    print("📊 Reading unique states and districts...")
    df_unique = pd.read_excel("Unique_States_Districts.xlsx")
    
    # Filter for Maharashtra only
    maharashtra_data = df_unique[df_unique['State'].str.contains('Maharashtra', case=False, na=False)]
    maharashtra_districts = maharashtra_data['District'].unique()
    
    print(f"✅ Found {len(maharashtra_districts)} unique districts in Maharashtra")
    
    # API configuration
    base_url = "https://indiawris.gov.in/Dataset/Ground Water Level"
    state_name = "maharashtra"
    agency_name = "CGWB"
    
    # Get yesterday's date (2025-09-20)
    yesterday = "2025-09-20"
    today = "2025-09-21"
    
    print(f"📅 Fetching data for date: {yesterday}")
    
    # Store all fetched data
    all_data = []
    
    # Headers to mimic browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://indiawris.gov.in/'
    }
    
    # Fetch data for each district
    districts_with_data = 0
    districts_without_data = 0
    
    for i, district in enumerate(maharashtra_districts):
        print(f"🔄 Processing district {i+1}/{len(maharashtra_districts)}: {district}")
        
        # API parameters
        params = {
            'stateName': state_name,
            'districtName': district,
            'agencyName': agency_name,
            'startdate': yesterday,
            'enddate': today,
            'download': 'false',
            'page': 0,
            'size': 1000
        }
        
        try:
            # Make API request with headers
            response = requests.post(base_url, params=params, headers=headers, timeout=30)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    
                    if data.get('statusCode') == 200 and data.get('data'):
                        stations = data['data']
                        print(f"   ✅ Found {len(stations)} stations")
                        districts_with_data += 1
                        
                        # Process each station
                        for station in stations:
                            station_data = {
                                'State': 'Maharashtra',
                                'District': district,
                                'Station_Code': station.get('stationCode', ''),
                                'Station_Name': station.get('stationName', ''),
                                'Latitude': station.get('latitude', ''),
                                'Longitude': station.get('longitude', ''),
                                'Well_Depth': station.get('wellDepth', ''),
                                'Data_Value': station.get('dataValue', ''),
                                'Data_Time': station.get('dataTime', ''),
                                'Unit': station.get('unit', ''),
                                'Well_Type': station.get('wellType', ''),
                                'Aquifer_Type': station.get('wellAquiferType', ''),
                                'Station_Status': station.get('stationStatus', '')
                            }
                            all_data.append(station_data)
                    else:
                        print(f"   ⚠️ No data found for {district}")
                        districts_without_data += 1
                        
                except json.JSONDecodeError as e:
                    print(f"   ❌ JSON decode error: {str(e)}")
            else:
                print(f"   ❌ API Error {response.status_code} for {district}")
                districts_without_data += 1
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Request failed for {district}: {str(e)}")
            districts_without_data += 1
        except Exception as e:
            print(f"   ❌ Error processing {district}: {str(e)}")
            districts_without_data += 1
        
        # Add delay to avoid overwhelming the API
        time.sleep(0.5)  # Reduced delay for faster processing
    
    # Create DataFrame from all data
    if all_data:
        df_result = pd.DataFrame(all_data)
        
        print(f"\n📊 Final Data Summary:")
        print(f"   Total stations fetched: {len(df_result)}")
        print(f"   Districts with data: {districts_with_data}")
        print(f"   Districts without data: {districts_without_data}")
        print(f"   Date range: {yesterday}")
        
        # Show sample data
        print(f"\n📋 Sample data:")
        print(df_result[['State', 'District', 'Station_Name', 'Latitude', 'Longitude', 'Well_Depth', 'Data_Value']].head(10).to_string(index=False))
        
        # Save to Excel
        output_file = f"ALL_Maharashtra_Groundwater_Data_{yesterday.replace('-', '_')}.xlsx"
        print(f"\n💾 Saving to Excel: {output_file}")
        df_result.to_excel(output_file, index=False, sheet_name='Groundwater_Data')
        
        # Also save to CSV
        csv_output = f"ALL_Maharashtra_Groundwater_Data_{yesterday.replace('-', '_')}.csv"
        print(f"💾 Saving to CSV: {csv_output}")
        df_result.to_csv(csv_output, index=False)
        
        # Show statistics
        print(f"\n📈 Statistics:")
        print(f"   Average well depth: {df_result['Well_Depth'].mean():.2f} meters")
        print(f"   Average data value: {df_result['Data_Value'].mean():.2f} meters")
        print(f"   Active stations: {len(df_result[df_result['Station_Status'] == 'Active'])}")
        
        # Show top districts by station count
        print(f"\n🏆 Top 10 districts by station count:")
        district_counts = df_result['District'].value_counts().head(10)
        for district, count in district_counts.items():
            print(f"   {district}: {count} stations")
        
        print(f"\n🎉 Data fetching completed successfully!")
        print(f"📁 Excel output: {output_file}")
        print(f"📁 CSV output: {csv_output}")
        
        return df_result
    else:
        print("❌ No data was fetched from the API")
        return None

if __name__ == "__main__":
    fetch_all_maharashtra_groundwater_data()
