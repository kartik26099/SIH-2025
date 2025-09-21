import pandas as pd 
import folium
from folium.plugins import HeatMap  

# Load your data
df = pd.read_excel("test.xlsx")  
print(df.columns)
# Create map centered on India
m = folium.Map(location=[20.5937, 78.9629], zoom_start=5)  


# Add heatmap layer
heat_data = [[row['LATITUDE  '], row['LONGITUDE '], row['DTWL']] 
             for index, row in df.iterrows()]  

HeatMap(heat_data, radius=15).add_to(m)  

# Save map
m.save("india_groundwater_heatmap.html")
