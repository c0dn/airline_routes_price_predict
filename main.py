import orjson
import random
import sys
import time
from concurrent import futures
from typing import List, Dict

import lxml.html
from curl_cffi import requests
from geopy.distance import geodesic


def load_proxies(filename='proxies.txt'):
    try:
        with open(filename, 'r') as f:
            proxies = [line.strip() for line in f if line.strip()]
        return proxies
    except FileNotFoundError:
        print(f"Error: {filename} not found. Please create a file with one proxy per line.")
        sys.exit(1)



def make_request(url, proxies, retry_count=3):
    for _ in range(retry_count):
        proxy = random.choice(proxies)
        try:
            response = requests.get(
                url,
                impersonate="chrome",
                proxies={
                    "http": f"socks5://{proxy}",
                    "https": f"socks5://{proxy}"
                }
            )
            return response
        except Exception as e:
            print(f"Proxy {proxy} failed: {str(e)}")
            continue
    raise Exception("All retry attempts failed")

def fetch_airport_data(iata: str, proxies: List[str]) -> Dict:
    print(f"Fetching airport: {iata}")

    while True:
        try:
            response = make_request(
                f"https://www.flightsfrom.com/{iata}/destinations",
                proxies
            )
            root = lxml.html.document_fromstring(response.content)
            metadata_nodes = root.xpath('//script[contains(., "window.airport")]')
            metadata_tag = metadata_nodes[0].text_content()
            metadata_bits = metadata_tag.split("window.")

            metadata = {}
            for bit in metadata_bits:
                split = bit.find("=")
                if split != -1:
                    metadata[bit[:split].strip()] = orjson.loads(bit.strip()[split + 2: -1])

            airport_fields = [
                "city_name", "continent", "country", "country_code",
                "display_name", "elevation", "IATA", "ICAO",
                "latitude", "longitude", "name", "timezone",
            ]

            airport = {
                field.lower(): metadata["airport"][field] for field in airport_fields
            }
            if airport["elevation"]:
                airport["elevation"] = int(airport["elevation"])

            routes = []
            new_iatas = []
            airlines = {}
            for route in metadata["routes"]:
                carrier_fields = ["name", "IATA"]

                carriers = []
                for aroute in route["airlineroutes"]:
                    is_passenger = (
                            aroute["airline"]["is_scheduled_passenger"] == "1"
                            or aroute["airline"]["is_nonscheduled_passenger"] == "1"
                    )
                    is_active = aroute["airline"]["active"]
                    if is_active and is_passenger:
                        carrier = {
                            field.lower(): aroute["airline"][field]
                            for field in carrier_fields
                        }
                        carriers.append(carrier)

                        if carrier.get("iata") and carrier.get("name"):
                            airlines[carrier["iata"]] = carrier["name"]

                orig_ll = (airport["latitude"], airport["longitude"])
                dest_ll = (route["airport"]["latitude"], route["airport"]["longitude"])
                distance = int(geodesic(orig_ll, dest_ll).km)

                routes.append(
                    {
                        "carriers": carriers,
                        "km": distance,
                        "min": int(route["common_duration"]),
                        "iata": route["iata_to"],
                    }
                )
                new_iatas.append(route["iata_to"])

            airport["routes"] = routes
            return {"iata": iata, "airport": airport, "new_iatas": new_iatas, "airlines": airlines}

        except Exception as e:
            print(f"! Error while fetching {iata}, retrying in 1m: {e}")
            time.sleep(60)


if __name__ == "__main__":

    proxies = load_proxies()
    print(f"Loaded {len(proxies)} SOCKS5 proxies from proxy.txt")


    print("Fetching airports list...")
    try:
        response = make_request("https://www.flightsfrom.com/airports", proxies)
        airports_json = orjson.loads(response.content)
    except Exception as e:
        print(f"Failed to load airport JSON: {str(e)}")
        sys.exit(1)



    iatas_all = set(airport["IATA"] for airport in airports_json["response"]["airports"])
    all_count = len(iatas_all)
    processed_iatas = set()
    airports = {}
    airline_mapping = {}
    MAX_WORKERS = 200

    with futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while iatas_all:
            current_batch = set()
            while len(current_batch) < MAX_WORKERS and iatas_all:
                iata = iatas_all.pop()
                if iata not in processed_iatas:
                    current_batch.add(iata)
            if not current_batch:
                break

            future_to_iata = {
                executor.submit(fetch_airport_data, iata, proxies): iata
                for iata in current_batch
            }

            for future in futures.as_completed(future_to_iata):
                try:
                    result = future.result()
                    iata = result["iata"]
                    airports[iata] = result["airport"]
                    processed_iatas.add(iata)

                    airline_mapping.update(result["airlines"])

                    # Add new IATAs to process
                    for new_iata in result["new_iatas"]:
                        if new_iata not in processed_iatas:
                            iatas_all.add(new_iata)

                    print(f"Completed {iata}, Total processed: {len(processed_iatas)}")
                except Exception as e:
                    print(f"Error processing {future_to_iata[future]}: {e}")


    print("Writing results to file...")
    with open("airline_routes.json", "w", encoding="utf-8") as f:
        f.write(orjson.dumps(airports, option=orjson.OPT_SORT_KEYS).decode("utf-8") + "\n")

    with open("airline_mapping.json", "w", encoding='utf-8') as f:
        f.write(orjson.dumps(airline_mapping, option=orjson.OPT_SORT_KEYS).decode("utf-8") + "\n")

    print(f"Completed! Processed {len(processed_iatas)}/{all_count} airports and {len(airline_mapping)} airlines.")