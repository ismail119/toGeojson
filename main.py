import pandas as pd
import argparse
import requests
import concurrent.futures
import json

def fetch_coordinates_from_address(address):
    try:
        url = f'https://maps.googleapis.com/maps/api/geocode/json?address={address}&key=AIzaSyBm3vfVhRylV0vpPPp_iMM5770nI8vzP6E'
        response = requests.get(url)

        if response.status_code != 200:
            raise Exception(f'HTTP error! Status: {response.status_code}')

        data = response.json()

        if data['status'] == 'ZERO_RESULTS':
            raise Exception('Geocoding API error! No results found for the provided address.')
        elif data['status'] != 'OK':
            raise Exception(f'Geocoding API error! Status: {data["status"]}')

        results = data['results']
        if results:
            location = results[0]['geometry']['location']
            latitude = location['lat']
            longitude = location['lng']
            return latitude, longitude
        else:
            return None, None
    except Exception as e:
        print(f"Error: {e}")
        return None, None

def main(file_name):
    try:
        if file_name.endswith('.xlsx'):
            data = pd.read_excel(file_name)
        elif file_name.endswith('.csv'):
            try:
                data = pd.read_csv(file_name, encoding='utf-8', delimiter=';')
            except UnicodeDecodeError:
                data = pd.read_csv(file_name, encoding='latin1', delimiter=';')
        else:
            raise ValueError("Unsupported file format. Please provide an Excel (.xlsx) or CSV (.csv) file.")

        if 'Adress' not in data.columns:
            raise ValueError("The file does not contain a column named 'Adress'.")

        print(f"Loaded {len(data)} addresses from {file_name}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            results = list(executor.map(fetch_coordinates_from_address, data['Adress']))

        data[['Latitude', 'Longitude']] = pd.DataFrame(results, index=data.index)
        data = data.dropna(subset=["Latitude", "Longitude"])

        json_result_string = data.to_json(orient='records', double_precision=12, date_format='iso')
        json_result = json.loads(json_result_string)

        geojson = {'type': 'FeatureCollection', 'features': []}
        for record in json_result:
            geojson['features'].append({
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [record['Longitude'], record['Latitude']],
                },
                'properties': record,
            })

        output_file = f'{file_name.rsplit(".", 1)[0]}.geojson'
        with open(output_file, 'w') as f:
            f.write(json.dumps(geojson, indent=2))

        print(f"GeoJSON data written to {output_file}")
        return output_file

    except Exception as e:
        print(f"Error in main: {e}")
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert Excel or CSV file to GeoJSON.")
    parser.add_argument("file_name", type=str, help="Name of the file to be converted")
    args = parser.parse_args()
    main(args.file_name)
