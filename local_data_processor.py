import pandas as pd
import os

# --- Configurations ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else "."
RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw", "assembly_line_iot_data.csv")
GOLD_DATA_DIR = os.path.join(BASE_DIR, "data", "gold", "kpi_metrics")

def run_local_processing():
    print(f"Reading raw data from: {RAW_DATA_PATH}")
    
    # Ingestion
    df = pd.read_csv(RAW_DATA_PATH)
    
    # Cleaning
    df = df.dropna()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Aggregations for Gold layer
    print("Aggregating KPIs for Gold layer...")
    
    kpi_df = df.groupby('machine_id').agg(
        avg_temperature=('temperature_c', 'mean'),
        max_temperature=('temperature_c', 'max'),
        avg_vibration=('vibration_hz', 'mean'),
        avg_cycle_time=('cycle_time_sec', 'mean')
    ).reset_index()
    
    anomalies_df = df[df['status'] == 'Anomaly'].groupby('machine_id').size().reset_index(name='anomaly_count')
    
    # Merge
    final_df = pd.merge(kpi_df, anomalies_df, on='machine_id', how='left').fillna(0)
    
    print(final_df)
    
    # Save
    os.makedirs(GOLD_DATA_DIR, exist_ok=True)
    gold_path = os.path.join(GOLD_DATA_DIR, "gold_iot_metrics.csv")
    final_df.to_csv(gold_path, index=False)
    
    print(f"Pipeline execution complete. Gold data saved to: {gold_path}")

if __name__ == "__main__":
    run_local_processing()
