import sys,argparse,yaml
from etl import ETL
import logging
from heaven import show_descending,show_ascending



def main():

    parser = argparse.ArgumentParser(description="Process a ZIP file for ETL pipeline") 
    parser.add_argument("--zip_path",  help="Path to the ZIP file containing in_network and provider data")
    parser.add_argument("--ApiD", action="store_true", help="Display planetary data in descending order")
    parser.add_argument("--ApiA", action="store_true", help="Display planetary data in ascending order")
    
    args = parser.parse_args()

    # zip_path = sys.argv[1]
    logging.basicConfig(level=logging.INFO)
    logger= logging.getLogger("ETL")

    if args.ApiD:
        show_descending()
    elif args.ApiA:
        show_ascending()
         
    if args.zip_path:
        etl = ETL(logger)
        etl.execute(args.zip_path)

    # etl = ETL(logger)
    # etl.execute(args.zip_path)




if __name__ == "__main__":
    main()


