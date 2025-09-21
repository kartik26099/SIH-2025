# Live Groundwater Data Pipeline - Flask Server

A complete Flask application that fetches groundwater data from the India WRIS API, stores it in Supabase, and serves a live interactive heatmap.

## 🚀 Features

- **Real-time Data Fetching**: Automatically fetches data from India WRIS API
- **Supabase Integration**: Stores data in your Supabase database
- **Live Heatmap**: Interactive web-based heatmap with real-time updates
- **Auto-refresh**: Updates data every 30 minutes automatically
- **Manual Controls**: Buttons to refresh data and update from API
- **Statistics**: Real-time statistics and groundwater status
- **Responsive Design**: Works on desktop and mobile devices

## 🏗️ Architecture

```
API (India WRIS) → Flask Server → Supabase → Live Heatmap
     ↓                ↓              ↓           ↓
  Fetch Data    →  Process Data  →  Store   →  Display
```

## 📁 Files

- `flask_groundwater_pipeline.py` - Main Flask application
- `requirements_flask.txt` - Python dependencies
- `supabase_config.env` - Supabase configuration
- `Unique_States_Districts.xlsx` - District data

## 🛠️ Setup

### 1. Install Dependencies

```bash
pip install -r requirements_flask.txt
```

### 2. Configure Supabase

Make sure your `supabase_config.env` is properly configured:

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your_anon_key_here
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key_here
SUPABASE_TABLE_NAME=maharashtra_groundwater_data
```

### 3. Run the Server

```bash
python flask_groundwater_pipeline.py
```

## 🌐 Usage

### Web Interface

1. **Open your browser** and go to: `http://localhost:5000`
2. **View the live heatmap** with groundwater data
3. **Use controls** to refresh data or update from API
4. **Toggle layers** to show/hide heatmap and markers

### API Endpoints

- `GET /` - Main heatmap page
- `GET /api/data` - Get current data from Supabase
- `POST /api/update` - Update data from API
- `GET /api/stats` - Get statistics

## 🎯 Features Explained

### 1. **Live Data Fetching**
- Fetches data from India WRIS API for all Maharashtra districts
- Processes and maps data to your database schema
- Handles API rate limiting and errors gracefully

### 2. **Supabase Integration**
- Automatically updates your Supabase database
- Clears old data and inserts fresh data
- Handles database errors and connection issues

### 3. **Interactive Heatmap**
- **Heatmap Layer**: Color-coded intensity based on well depth
- **Marker Layer**: Individual station markers with popups
- **Layer Controls**: Toggle between heatmap and markers
- **Real-time Updates**: Auto-refreshes every 5 minutes

### 4. **Statistics Dashboard**
- Total number of stations
- Average well depth
- Breakdown by groundwater status:
  - 🟢 Good (0-5m)
  - 🔵 Moderate (5-15m)
  - 🟡 Poor (15-30m)
  - 🔴 Critical (>30m)

### 5. **Auto-scheduling**
- Updates data every 30 minutes automatically
- Runs in background thread
- Can be manually triggered via web interface

## 🎨 Map Features

### Color Coding
- **Blue → Cyan → Green → Yellow → Orange → Red**
- Lower well depth = Better groundwater = Red/Orange
- Higher well depth = Worse groundwater = Blue

### Interactive Elements
- **Click markers** for station details
- **Zoom and pan** to explore different areas
- **Toggle layers** for different views
- **Real-time statistics** in control panel

## 🔧 Configuration

### Environment Variables

```env
# Supabase Configuration
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your_anon_key_here
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key_here
SUPABASE_TABLE_NAME=maharashtra_groundwater_data

# API Configuration
API_BASE_URL=https://indiawris.gov.in/Dataset/Ground Water Level
API_STATE_NAME=maharashtra
API_AGENCY_NAME=CGWB
```

### Customization Options

1. **Update Frequency**: Change `schedule.every(30).minutes` to your preferred interval
2. **Districts**: Modify `demo_districts = self.districts[:20]` to include more/fewer districts
3. **Map Center**: Change `[20.5937, 78.9629]` to center on different location
4. **Colors**: Modify the gradient in the heatmap configuration

## 📊 Data Flow

1. **Scheduled Update** (every 30 minutes):
   - Fetch data from India WRIS API
   - Process and map to database schema
   - Update Supabase database
   - Clear old data and insert new data

2. **Web Interface** (real-time):
   - Fetch data from Supabase
   - Generate heatmap and markers
   - Update statistics
   - Auto-refresh every 5 minutes

3. **Manual Update** (on-demand):
   - User clicks "Update from API" button
   - Triggers immediate data fetch and update
   - Refreshes the map with new data

## 🚨 Error Handling

- **API Failures**: Continues processing other districts
- **Database Errors**: Logs errors and continues operation
- **Network Issues**: Retries with exponential backoff
- **Invalid Data**: Filters out missing coordinates/depths

## 🔍 Monitoring

The application provides detailed logging:
- ✅ Success indicators
- ❌ Error messages
- 📊 Progress updates
- 🔄 Update status

## 🎉 Benefits

1. **Real-time Monitoring**: Always up-to-date groundwater data
2. **Scalable**: Can easily add more states/districts
3. **User-friendly**: Simple web interface
4. **Automated**: No manual intervention required
5. **Reliable**: Handles errors gracefully
6. **Interactive**: Rich visualization with statistics

---

**Ready to monitor groundwater levels in real-time!** 🌊📊
