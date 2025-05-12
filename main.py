import sys,argparse
from extract import extract
from transform import transform
from load import load

def main():
    parser = argparse.ArgumentParser(description="Process a ZIP file for ETL pipeline") 
    parser.add_argument("--location", required=True, help="Path to the ZIP file containing in_network and provider data")
    args = parser.parse_args()
    
    zip_path = args.location
    # zip_path = sys.argv[1]

    provider_path,inetwork_path = extract.extract_it(zip_path)
    rate1_path,provider1_path = transform.trasform_loc(provider_path,inetwork_path)
    load.load(rate1_path,provider1_path)

if __name__ == "__main__":
    main()