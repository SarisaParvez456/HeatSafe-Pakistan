"""Safely extend Karachi weather history to 2020-2021; intended for testing branch."""
from pathlib import Path
import subprocess
import sys
import requests
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / 'data' / 'raw'
METEO = RAW / 'open_meteo_karachi_2022_2026.csv'
NASA = RAW / 'nasa_power_karachi_2022_2026.csv'
METEO_COLS = ['time','temperature_2m','relative_humidity_2m','dew_point_2m','apparent_temperature','wind_speed_10m']
NASA_COLS = ['time','T2M','RH2M','WS2M']
LAT, LON = 24.8607, 67.0011
START, END = pd.Timestamp('2020-01-01',tz='UTC'), pd.Timestamp('2022-01-01',tz='UTC')
EXPECTED_NEW = 17544

def fetch(url, params):
    r = requests.get(url, params=params, timeout=180)
    r.raise_for_status()
    payload = r.json()
    if payload.get('error'):
        raise RuntimeError(payload.get('reason', 'API error'))
    return payload

def meteo(year):
    p = fetch('https://archive-api.open-meteo.com/v1/archive', {
        'latitude':LAT,'longitude':LON,'start_date':f'{year}-01-01',
        'end_date':f'{year}-12-31','timezone':'UTC','models':'era5',
        'hourly':','.join(METEO_COLS[1:])})
    return pd.DataFrame(p['hourly'])[METEO_COLS]

def nasa(year):
    p = fetch('https://power.larc.nasa.gov/api/temporal/hourly/point', {
        'parameters':'T2M,RH2M,WS2M','community':'RE',
        'longitude':LON,'latitude':LAT,'start':f'{year}0101',
        'end':f'{year}1231','format':'JSON','time-standard':'UTC'})['properties']['parameter']
    frame = pd.DataFrame({c:pd.Series(p[c]) for c in NASA_COLS[1:]})
    frame.index = pd.to_datetime(frame.index, format='%Y%m%d%H', utc=True)
    frame.index.name = 'time'
    return frame.reset_index()[NASA_COLS]

def normalize(frame, cols):
    frame = frame[cols].copy()
    frame['time'] = pd.to_datetime(frame['time'],utc=True)
    for col in cols[1:]:
        frame[col] = pd.to_numeric(frame[col],errors='coerce')
        frame.loc[frame[col] <= -900,col] = float('nan')
    if frame['time'].duplicated().any():
        raise ValueError('Duplicate hourly timestamps')
    return frame.sort_values('time').reset_index(drop=True)

def prepare(path, years, cols):
    old = normalize(pd.read_csv(path),cols)
    if len(old) != 41616:
        raise ValueError(f'{path.name}: expected original 41,616 rows, found {len(old)}; refusing to overwrite')
    new = normalize(pd.concat(years,ignore_index=True),cols)
    expected = pd.date_range(START, END-pd.Timedelta(hours=1),freq='h',tz='UTC')
    if len(new)!=EXPECTED_NEW or not new['time'].equals(pd.Series(expected,name='time')):
        raise ValueError(f'{path.name}: missing or duplicated 2020-2021 hours ({len(new)} rows)')
    if new[cols[1:]].isna().any().any():
        raise ValueError(f'{path.name}: API returned missing readings; refusing to overwrite')
    if old['time'].min() < END:
        raise ValueError(f'{path.name}: existing data overlaps 2020-2021; refusing to overwrite')
    combined = pd.concat([new,old],ignore_index=True)
    if len(combined)!=59160:
        raise ValueError(f'{path.name}: incorrect combined row count')
    return combined

def main():
    # No original files are changed until both downloads and validation succeed.
    for path in (METEO,NASA):
        if not path.exists():
            raise FileNotFoundError(path)
    m = prepare(METEO,[meteo(y) for y in (2020,2021)],METEO_COLS)
    n = prepare(NASA,[nasa(y) for y in (2020,2021)],NASA_COLS)
    m.to_csv(METEO,index=False)
    n.to_csv(NASA,index=False)
    subprocess.run([sys.executable,'src/data_pipeline.py'],cwd=ROOT,check=True)
    processed_path = ROOT/'data/processed/heatsafe_processed_weather.csv'
    processed = pd.read_csv(processed_path)
    if len(processed)!=59160:
        raise ValueError(f'Processed dataset has {len(processed)} rows, expected 59,160')
    processed.to_csv(ROOT/'data/processed/heatsafe_ml_dataset.csv',index=False)
    print('SUCCESS: 59,160 rows in raw and processed datasets. ML model unchanged.')

if __name__=='__main__':
    main()
