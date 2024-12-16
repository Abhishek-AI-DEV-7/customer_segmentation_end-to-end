import os
import zipfile
from abc import abstractmethod, ABC
import pandas as pd


class DataIngestor(ABC):
    @abstractmethod
    def ingest_data(self, file_path: str) -> pd.DataFrame:
        """Abstract method to ingest data from file."""
        pass


def find_csv_files(directory: str) -> list:
    """Recursively find all CSV files in a directory."""
    csv_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".csv"):
                csv_files.append(os.path.join(root, file))
    return csv_files


class ZipDataIngestor(DataIngestor):
    def ingest_data(self, file_path: str) -> pd.DataFrame:
        """Extract .zip file and return content as DataFrame."""
        
        # Ensure file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file {file_path} does not exist")
        
        # Ensure the file is a ZIP file
        if not file_path.endswith(".zip"):
            raise ValueError(f"{file_path} is not a ZIP file")
        
        # Create a directory for extracted data
        extract_dir = os.path.join(os.path.dirname(file_path), "extracted_data")
        os.makedirs(extract_dir, exist_ok=True)
        
        try:
            # Extract the ZIP file
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                zip_ref.extractall(extract_dir)
            
            # Debug extracted directory structure
            print("Extracted directory structure:")
            for root, dirs, files in os.walk(extract_dir):
                print(f"Root: {root}, Dirs: {dirs}, Files: {files}")
            
            # Find all CSV files
            csv_files = find_csv_files(extract_dir)
            print(f"CSV files found: {csv_files}")
            
            if len(csv_files) == 0:
                raise FileNotFoundError("No CSV file found in the extracted ZIP file")
            
            # Select the first CSV file for processing (customize if needed)
            csv_file_path = csv_files[0]
            print(f"Using CSV file: {csv_file_path}")
            
            # Read the selected CSV into a DataFrame
            df = pd.read_csv(csv_file_path)
            
            return df
        
        except zipfile.BadZipFile:
            raise ValueError(f"The file {file_path} is not a valid ZIP file")
        
        finally:
            # Optional cleanup of extracted files (comment this out during debugging)
            if os.path.exists(extract_dir):
                for root, _, files in os.walk(extract_dir, topdown=False):
                    for file in files:
                        os.remove(os.path.join(root, file))
                    os.rmdir(root)


class DataIngestionFactory:
    @staticmethod
    def get_data_ingestor(file_extension: str) -> DataIngestor:
        """Returns the appropriate DataIngestor based on file extension."""
        if file_extension == ".zip":
            return ZipDataIngestor()
        else:
            raise ValueError(f"No ingestor available for file extension: {file_extension}")


if __name__ == "__main__":
    # Use WSL path or correct file path
    file_path = "/home/customer-segmentation-mlops/Data/bank_marketing.zip"
    
    try:
        # Determine file extension
        file_extension = os.path.splitext(file_path)[1]
        
        # Get appropriate DataIngestor
        data_ingestor = DataIngestionFactory.get_data_ingestor(file_extension)
        
        # Ingest the data and load it into a DataFrame
        df = data_ingestor.ingest_data(file_path)
        
        # Print the resulting DataFrame
        print(f"Successfully loaded DataFrame from {file_path}")
        print(df.head())
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please ensure the ZIP file exists in the specified directory and contains CSV files.")
    except Exception as e:
        print(f"Error: {str(e)}")
