import sys
from extract import extract
from transform import transform
from load import load

def main():
    zip_path = sys.argv[1]

    provider_path,inetwork_path = extract.extract_it(zip_path)
    rate1_path,provider1_path = transform.transform_loc(provider_path,inetwork_path)
    load.load(rate1_path,provider1_path)

if __name__ == "__main__":
    main()