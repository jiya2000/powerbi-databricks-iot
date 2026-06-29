import csv
import random
import os
from datetime import datetime, timedelta

def generate_iot_data(num_records, output_dir="data/raw"):
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, "assembly_line_iot_data.csv")
    
    machines = ["Welding_Robot_1", "Welding_Robot_2", "Paint_Booth_A", "Assembly_Station_X"]
    
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["timestamp", "machine_id", "temperature_c", "vibration_hz", "cycle_time_sec", "status"])
        
        start_time = datetime.now() - timedelta(days=7)
        
        for i in range(num_records):
            machine = random.choice(machines)
            timestamp = (start_time + timedelta(minutes=i*5)).strftime("%Y-%m-%d %H:%M:%S")
            
            # Base values
            temp = random.uniform(40.0, 70.0)
            vib = random.uniform(10.0, 50.0)
            cycle = random.uniform(45.0, 60.0)
            status = "Normal"
            
            # Introduce anomalies
            if random.random() < 0.05:  # 5% chance of anomaly
                status = "Anomaly"
                if machine.startswith("Welding"):
                    temp = random.uniform(75.0, 95.0) # Overheating
                elif machine.startswith("Paint"):
                    vib = random.uniform(60.0, 100.0) # High vibration
                else:
                    cycle = random.uniform(80.0, 120.0) # Delay
            
            writer.writerow([timestamp, machine, round(temp, 2), round(vib, 2), round(cycle, 2), status])
            
    print(f"Generated {num_records} records in {file_path}")

if __name__ == "__main__":
    generate_iot_data(5000)
