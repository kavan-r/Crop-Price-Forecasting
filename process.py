

import pandas as pd
import os

# Paths
processed_path = "data/data/processed"
daily_output_file = "Crop_price_forecast_Daily.xlsx"
weekly_output_file = "Crop_price_forecast_Weekly.xlsx"

# Crops in desired order
crops = ["capsicum", "onion", "tomato", "wheat"]

def process_crop(crop):
    """
    Reads all CSVs for a crop, standardizes columns, adds crop_type,
    computes full_state rows, moving averages, and sorts by district and date.
    Returns the combined DataFrame and the original district order.
    """
    # Read all CSVs for the crop
    files = [f for f in os.listdir(processed_path) if crop in f and f.endswith(".csv")]
    if not files:
        print(f"No CSV found for {crop}")
        return None, []
    
    df_list = []
    for file in files:
        df = pd.read_csv(os.path.join(processed_path, file))
        df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)
        df_list.append(df)
    
    crop_df = pd.concat(df_list, ignore_index=True)
    
    # Standardize column names
    crop_df.rename(columns={
        'Modal Price': 'modal_price',
        'Min Price': 'min_price',
        'Max Price': 'max_price',
        'Average Price': 'average',
        'District': 'district'
    }, inplace=True)
    
    crop_df['crop_type'] = crop
    
    # Preserve district order
    district_order = crop_df['district'].drop_duplicates().tolist()
    crop_df['district'] = pd.Categorical(crop_df['district'], categories=district_order, ordered=True)
    
    # Compute full_state rows (average across districts)
    full_state_df = crop_df.groupby('Date')[['min_price','max_price','modal_price','average']].mean().reset_index()
    full_state_df['district'] = 'All Districts'
    full_state_df['crop_type'] = 'full_state'
    
    # Combine district-level and full_state
    combined_df = pd.concat([crop_df, full_state_df], ignore_index=True)
    
    # Compute moving averages per crop_type
    combined_df['moving_avg_7'] = combined_df.groupby('crop_type')['modal_price'].transform(lambda x: x.rolling(7, min_periods=1).mean())
    combined_df['moving_avg_30'] = combined_df.groupby('crop_type')['modal_price'].transform(lambda x: x.rolling(30, min_periods=1).mean())
    
    # Sort by district order and date
    combined_df.sort_values(by=['district','Date'], inplace=True)
    
    return combined_df, district_order

# ------------------ Process all crops ------------------ #
all_crops_list = []
district_orders = {}  # store district order per crop

for crop in crops:
    df_crop, order = process_crop(crop)
    all_crops_list.append(df_crop)
    district_orders[crop] = order

all_crops_df = pd.concat(all_crops_list, ignore_index=True)

# Split full_state rows
full_state_rows = all_crops_df[all_crops_df['crop_type'] == 'full_state']
district_rows = all_crops_df[all_crops_df['crop_type'] != 'full_state']

# ------------------ Daily Data ------------------ #
final_daily_list = []
for crop in crops:
    crop_districts = district_rows[district_rows['crop_type'] == crop]
    crop_full_state = full_state_rows[full_state_rows['Date'].isin(crop_districts['Date'])]
    
    # Reorder districts as per original CSV
    crop_districts['district'] = pd.Categorical(crop_districts['district'], categories=district_orders[crop], ordered=True)
    crop_districts.sort_values(['district','Date'], inplace=True)
    
    final_daily_list.append(crop_districts)
    final_daily_list.append(crop_full_state)

daily_df = pd.concat(final_daily_list, ignore_index=True)
daily_df['Date'] = daily_df['Date'].dt.date  # Remove time
cols = ['Date', 'district', 'crop_type', 'min_price', 'max_price', 'modal_price', 'average', 'moving_avg_7', 'moving_avg_30']
daily_df = daily_df[cols]
daily_df.to_excel(daily_output_file, index=False)
print(f"Daily Excel saved: {daily_output_file}")

# ------------------ Weekly Data ------------------ #
weekly_df = daily_df.copy()
weekly_df['Date'] = pd.to_datetime(weekly_df['Date'])
weekly_df.set_index('Date', inplace=True)

# Resample weekly per crop_type and district
weekly_df = weekly_df.groupby(['crop_type','district']).resample('W').mean().reset_index()

# Reconstruct weekly order same as daily
final_weekly_list = []
for crop in crops:
    crop_districts = weekly_df[weekly_df['crop_type'] == crop]
    # Split districts and full_state
    districts_rows = crop_districts[crop_districts['district'] != 'All Districts']
    full_state_rows_crop = crop_districts[crop_districts['district'] == 'All Districts']
    
    # Reorder districts
    districts_rows['district'] = pd.Categorical(districts_rows['district'], categories=district_orders[crop], ordered=True)
    districts_rows.sort_values(['district','Date'], inplace=True)
    
    final_weekly_list.append(districts_rows)
    final_weekly_list.append(full_state_rows_crop)

weekly_df = pd.concat(final_weekly_list, ignore_index=True)
weekly_df['Date'] = weekly_df['Date'].dt.date
weekly_df = weekly_df[cols]
weekly_df.to_excel(weekly_output_file, index=False)
print(f"Weekly Excel saved: {weekly_output_file}")
