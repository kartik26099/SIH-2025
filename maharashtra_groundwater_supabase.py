import pandas as pd
import requests
import time
import json
import os
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv('supabase_config.env')

class MaharashtraGroundwaterSupabase:
    def __init__(self):
        """Initialize the groundwater data fetcher and Supabase client"""
        print("🚀 Initializing Maharashtra Groundwater Supabase Integration...")
        
        # Load configuration from environment
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_KEY')
        self.supabase_table = os.getenv('SUPABASE_TABLE_NAME', 'maharashtra_groundwater_data')
        
        self.api_base_url = os.getenv('API_BASE_URL', 'https://indiawris.gov.in/Dataset/Ground Water Level')
        self.api_state_name = os.getenv('API_STATE_NAME', 'maharashtra')
        self.api_agency_name = os.getenv('API_AGENCY_NAME', 'CGWB')
        self.data_date = os.getenv('DATA_DATE', '2025-09-20')
        self.data_end_date = os.getenv('DATA_END_DATE', '2025-09-21')
        
        # Validate configuration
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("❌ Supabase URL and Key must be set in supabase_config.env")
        
        # Initialize Supabase client
        try:
            self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
            print("✅ Supabase client initialized successfully")
        except Exception as e:
            raise Exception(f"❌ Failed to initialize Supabase client: {str(e)}")
        
        # API headers
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://indiawris.gov.in/'
        }
        
        print("✅ Configuration loaded successfully")
    
    def load_maharashtra_districts(self):
        """Load unique Maharashtra districts from Excel file"""
        print("📊 Loading Maharashtra districts...")
        
        try:
            df_unique = pd.read_excel("Unique_States_Districts.xlsx")
            maharashtra_data = df_unique[df_unique['State'].str.contains('Maharashtra', case=False, na=False)]
            maharashtra_districts = maharashtra_data['District'].unique()
            
            print(f"✅ Found {len(maharashtra_districts)} unique districts in Maharashtra")
            return maharashtra_districts
        except Exception as e:
            raise Exception(f"❌ Failed to load districts: {str(e)}")
    
    def fetch_district_data(self, district):
        """Fetch groundwater data for a specific district"""
        params = {
            'stateName': self.api_state_name,
            'districtName': district,
            'agencyName': self.api_agency_name,
            'startdate': self.data_date,
            'enddate': self.data_end_date,
            'download': 'false',
            'page': 0,
            'size': 1000
        }
        
        try:
            response = requests.post(self.api_base_url, params=params, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('statusCode') == 200 and data.get('data'):
                    return data['data']
                else:
                    return []
            else:
                print(f"   ❌ API Error {response.status_code} for {district}")
                return []
                
        except Exception as e:
            print(f"   ❌ Request failed for {district}: {str(e)}")
            return []
    
    def map_api_to_database_schema(self, station_data, district):
        """Map API response to database schema exactly as shown in the image"""
        return {
            'state': 'Maharashtra',  # text
            'district': district,    # text
            'station_code': station_data.get('stationCode', ''),  # text
            'station_name': station_data.get('stationName', ''),  # text
            'latitude': float(station_data.get('latitude', 0)) if station_data.get('latitude') else None,  # float8
            'longitude': float(station_data.get('longitude', 0)) if station_data.get('longitude') else None,  # float8
            'well_depth': float(station_data.get('wellDepth', 0)) if station_data.get('wellDepth') else None,  # float8
            'data_value': float(station_data.get('dataValue', 0)) if station_data.get('dataValue') else None,  # float8
            'data_time': station_data.get('dataTime', ''),  # timestamp
            'unit': station_data.get('unit', ''),  # text
            'well_type': station_data.get('wellType', ''),  # text
            'aquifer_type': station_data.get('wellAquiferType', ''),  # text
            'station_status': station_data.get('stationStatus', '')  # text
        }
    
    def upload_to_supabase(self, data_batch):
        """Upload a batch of data to Supabase"""
        try:
            # Clear existing data for the date (optional - you can comment this out if you want to keep historical data)
            # self.supabase.table(self.supabase_table).delete().eq('data_time', f"{self.data_date}T00:00:00").execute()
            
            # Insert new data
            result = self.supabase.table(self.supabase_table).insert(data_batch).execute()
            return result
        except Exception as e:
            print(f"   ❌ Supabase upload error: {str(e)}")
            return None
    
    def process_all_districts(self):
        """Process all Maharashtra districts and upload to Supabase"""
        print("🚀 Starting Maharashtra groundwater data processing...")
        
        # Load districts
        districts = self.load_maharashtra_districts()
        
        all_data = []
        districts_with_data = 0
        districts_without_data = 0
        total_stations = 0
        
        print(f"📅 Processing data for date: {self.data_date}")
        print(f"🔄 Processing {len(districts)} districts...")
        
        # Process each district
        for i, district in enumerate(districts):
            print(f"🔄 Processing district {i+1}/{len(districts)}: {district}")
            
            # Fetch data for district
            stations = self.fetch_district_data(district)
            
            if stations:
                print(f"   ✅ Found {len(stations)} stations")
                districts_with_data += 1
                total_stations += len(stations)
                
                # Map data to database schema
                for station in stations:
                    mapped_data = self.map_api_to_database_schema(station, district)
                    all_data.append(mapped_data)
            else:
                print(f"   ⚠️ No data found for {district}")
                districts_without_data += 1
            
            # Add delay to avoid overwhelming the API
            time.sleep(0.5)
        
        # Upload to Supabase in batches
        if all_data:
            print(f"\n📊 Data Summary:")
            print(f"   Total stations fetched: {len(all_data)}")
            print(f"   Districts with data: {districts_with_data}")
            print(f"   Districts without data: {districts_without_data}")
            
            # Upload in batches of 100
            batch_size = 100
            total_batches = (len(all_data) + batch_size - 1) // batch_size
            
            print(f"\n💾 Uploading to Supabase in {total_batches} batches...")
            
            successful_uploads = 0
            failed_uploads = 0
            
            for i in range(0, len(all_data), batch_size):
                batch = all_data[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                print(f"   📤 Uploading batch {batch_num}/{total_batches} ({len(batch)} records)...")
                
                result = self.upload_to_supabase(batch)
                
                if result:
                    successful_uploads += len(batch)
                    print(f"   ✅ Batch {batch_num} uploaded successfully")
                else:
                    failed_uploads += len(batch)
                    print(f"   ❌ Batch {batch_num} failed to upload")
                
                # Small delay between batches
                time.sleep(1)
            
            print(f"\n🎉 Upload completed!")
            print(f"   ✅ Successfully uploaded: {successful_uploads} records")
            print(f"   ❌ Failed uploads: {failed_uploads} records")
            print(f"   📊 Total records processed: {len(all_data)}")
            
            # Show sample data
            print(f"\n📋 Sample uploaded data:")
            if all_data:
                sample = all_data[0]
                for key, value in sample.items():
                    print(f"   {key}: {value}")
            
            return True
        else:
            print("❌ No data was fetched from the API")
            return False
    
    def verify_upload(self):
        """Verify the data was uploaded correctly"""
        try:
            print("\n🔍 Verifying upload...")
            result = self.supabase.table(self.supabase_table).select("*").limit(5).execute()
            
            if result.data:
                print(f"✅ Verification successful - found {len(result.data)} records in database")
                print("📋 Sample record from database:")
                sample = result.data[0]
                for key, value in sample.items():
                    print(f"   {key}: {value}")
                return True
            else:
                print("❌ No records found in database")
                return False
        except Exception as e:
            print(f"❌ Verification failed: {str(e)}")
            return False

def main():
    """Main function to run the groundwater data processing"""
    try:
        # Initialize the processor
        processor = MaharashtraGroundwaterSupabase()
        
        # Process all districts and upload to Supabase
        success = processor.process_all_districts()
        
        if success:
            # Verify the upload
            processor.verify_upload()
            print("\n🎉 Maharashtra groundwater data processing completed successfully!")
        else:
            print("\n❌ Processing failed!")
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    main()
