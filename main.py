import sys,argparse,yaml
from etlnrpr import ETL
import logging
def main():
    parser = argparse.ArgumentParser(description="Process a ZIP file for ETL pipeline")
    # parser.add_argument("--zip_path",  help="Path to the ZIP file containing in_network and provider data")
    # parser.add_argument("--provider", help="Path to provider_detail.json")
    parser.add_argument("--prov", help="Path to provider_detail.json")

    parser.add_argument("--zip", help="Path to the ZIP file to process")
    args = parser.parse_args()
    # zip_path = sys.argv[1]
    logging.basicConfig(level=logging.INFO,filename="etl.log")
    logger= logging.getLogger("ETL")
    etl = ETL(logger)
    etl.execute(args.zip,args.prov)
if __name__ == "__main__":
    main()