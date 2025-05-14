import sys,argparse,yaml
from etl import ETL
import logging



def main():

    parser = argparse.ArgumentParser(description="Process a ZIP file for ETL pipeline") 
    parser.add_argument("--zip_path", required=True, help="Path to the ZIP file containing in_network and provider data")
    args = parser.parse_args()

    # zip_path = sys.argv[1]
    logging.basicConfig(level=logging.INFO)
    logger= logging.getLogger("ETL")
    etl = ETL(logger)
    etl.execute(args.zip_path)




if __name__ == "__main__":
    main()


