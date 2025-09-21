# Maharashtra Groundwater Data - Supabase Integration

This project fetches real-time groundwater data for all Maharashtra districts and uploads it to a Supabase database.

## 🗄️ Database Schema

The data is stored in the `maharashtra_groundwater_data` table with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `id` | int8 | Primary key (auto-increment) |
| `state` | text | State name (Maharashtra) |
| `district` | text | District name |
| `station_code` | text | Station code |
| `station_name` | text | Station name |
| `latitude` | float8 | Latitude coordinate |
| `longitude` | float8 | Longitude coordinate |
| `well_depth` | float8 | Well depth in meters |
| `data_value` | float8 | Groundwater level data value |
| `data_time` | timestamp | Data timestamp |
| `unit` | text | Unit of measurement |
| `well_type` | text | Type of well |
| `aquifer_type` | text | Aquifer type |
| `station_status` | text | Station status |

## 🚀 Setup Instructions

### 1. Install Dependencies

```bash
python setup_supabase.py
```

Or manually install:

```bash
pip install -r requirements_supabase.txt
```

### 2. Configure Supabase

1. Update `supabase_config.env` with your Supabase credentials:
   ```env
   SUPABASE_URL=https://your-project.supabase.co
   SUPABASE_KEY=your_anon_key_here
   SUPABASE_SERVICE_ROLE_KEY=your_service_role_key_here
   SUPABASE_TABLE_NAME=maharashtra_groundwater_data
   ```

2. Create the table in your Supabase database with the schema shown above.

### 3. Required Files

Make sure you have:
- `Unique_States_Districts.xlsx` - Contains all Maharashtra districts
- `supabase_config.env` - Your Supabase configuration

## 🏃‍♂️ Running the Script

```bash
python maharashtra_groundwater_supabase.py
```

## 📊 What the Script Does

1. **Loads Districts**: Reads all Maharashtra districts from `Unique_States_Districts.xlsx`
2. **Fetches Data**: Calls the India WRIS API for each district
3. **Maps Data**: Converts API response to match database schema exactly
4. **Uploads to Supabase**: Sends data in batches to your Supabase table
5. **Verifies Upload**: Confirms data was uploaded successfully

## 🔧 Configuration Options

You can modify these settings in `supabase_config.env`:

- `DATA_DATE`: Date to fetch data for (default: 2025-09-20)
- `DATA_END_DATE`: End date for data range (default: 2025-09-21)
- `API_STATE_NAME`: State name for API (default: maharashtra)
- `API_AGENCY_NAME`: Agency name for API (default: CGWB)

## 📈 Expected Results

The script will:
- Process all 216+ Maharashtra districts
- Fetch groundwater data for each district with available data
- Upload all data to your Supabase table
- Provide detailed progress and statistics

## 🛠️ Troubleshooting

### Common Issues:

1. **Supabase Connection Error**: Check your URL and keys in `supabase_config.env`
2. **API Errors**: The script handles API failures gracefully and continues
3. **Missing Excel File**: Ensure `Unique_States_Districts.xlsx` exists
4. **Permission Errors**: Make sure your Supabase key has insert permissions

### Logs:

The script provides detailed logging:
- ✅ Success indicators
- ❌ Error messages
- 📊 Progress updates
- 🔍 Verification results

## 📁 Files Created

- `maharashtra_groundwater_supabase.py` - Main script
- `supabase_config.env` - Configuration file
- `requirements_supabase.txt` - Dependencies
- `setup_supabase.py` - Setup helper
- `README_Supabase_Integration.md` - This documentation

## 🎯 Next Steps

After successful upload, you can:
1. Query the data in Supabase dashboard
2. Create visualizations
3. Set up automated data fetching
4. Build applications using the groundwater data

---

**Note**: The script preserves the exact API response format and database schema as specified. No modifications are made to the data structure.
