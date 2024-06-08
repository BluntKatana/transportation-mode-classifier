import pandas as pd
import numpy as np
import os

class ExportPhyphoxData:
    def __init__(self):
        pass

    def combine_from_folder(self, label, folder_path, output_path = None):
        """
        Combine all the data from the phyphox folder into a single dataframe
        which can be used for further processing

        Parameters
        ----------
        folder_path : str
            The path to the folder containing the phyphox data files
        label : 'bike' | 'car' | 'walk' | 'train'
            The label for the data (expirement)
        output_path : str
            The path to save the combined data to. If None, the data will not be saved
        """
        data_files = {
            "accelerometer": "Accelerometer.csv",
            "gyroscope": "Gyroscope.csv",
            "light": "Light.csv",
            "linear_acceleration": "Linear Acceleration.csv",
            "location": "Location.csv",
            "magnetometer": "Magnetometer.csv",
            "pressure": "Pressure.csv",
            "proximity": "Proximity.csv",
            "temperature": "Temperature.csv",
        }

        dataframes = {}
        for name, file in data_files.items():
            # make sure the file exists before trying to read it
            try:
                dataframes[name] = pd.read_csv(folder_path + file, delimiter=";")

                # make sure the time column is considered as a float
                dataframes[name]["Time (s)"] = dataframes[name]['Time (s)'].str.replace(',', '.').replace("E", "e")
                dataframes[name]["Time (s)"].astype(float)
            except FileNotFoundError:
                print(f"\tFile {file} not found. Skipping...")
                continue

        # Merge dataframes into a single dataframe
        combined_df = None
        for name, df in dataframes.items():
            if combined_df is None:
                combined_df = df
            else:
                # Convert "Time (s)" column from scientific to float notation
                combined_df = pd.concat([combined_df, df])
                # merged_df = merged_df.merge(df, how='outer', on='Time (s)')

        # make sure the time column is considered as a float
        combined_df["Time (s)"] = combined_df["Time (s)"].str.replace(',', '.').replace("E", "e")
        combined_df["Time (s)"] = combined_df["Time (s)"].astype(float)

        # Add some additional columns
        # - grab the start_date if it exists, otherwise use the first timestamp
        meta_data = pd.read_csv(folder_path + "meta/time.csv", delimiter=";")
        START_DATE = "1970-01-01 00:00:00"
        if "START_DATE" in meta_data.columns:
            START_DATE = meta_data.iloc[0, 3]

        combined_df["timestamp"] = pd.to_timedelta(combined_df["Time (s)"], unit='s') + pd.to_datetime(START_DATE)
        combined_df["transportation_mode"] = label
        combined_df["start_date"] = START_DATE
        combined_df["expirement_id"] = np.random.randint(0, 1000000)

        # Save the combined dataframe to a csv file
        if output_path is not None:
            combined_df.to_csv(f"{output_path}{label} {START_DATE}.csv", index=False)

        return combined_df
    
    def combine_from_multiple_folders(self, label, folders_path, output_path = None):
        """
        Combine all the data from the phyphox folders into a single dataframe
        which can be used for further processing
        """
        dataframes = []

        # find all folders inside the folders_path
        folders = [f for f in os.listdir(folders_path) if os.path.isdir(os.path.join(folders_path, f))]
        for folder in folders:
            print(f"Combining data from folder: {folder}")
            dataframes.append(self.combine_from_folder(label, f"{folders_path}{folder}/"))

        if len(dataframes) == 0:
            print("!! No data found in the folders. Exiting... !!")
            return
        # Merge dataframes into a single dataframe
        combined_df = pd.concat(dataframes)

        # Save the combined dataframe to a csv file
        if output_path is not None:
            combined_df.to_csv(f"{output_path}{label}.csv", index=False)

        return combined_df


if __name__ == "__main__":
    data_preparation = ExportPhyphoxData()
    for label in ["bike", "car", "walk", "train"]:
        data_preparation.combine_from_multiple_folders(label, f"data/{label}/", "data/")