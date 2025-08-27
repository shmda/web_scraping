import pandas as pd
import numpy as np
import yaml
import time
import re
import json


from rapidfuzz import process, fuzz
from bs4 import BeautifulSoup as bs
from multiprocessing import Pool, cpu_count
from playwright.sync_api import sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from functools import partial
import sqlalchemy as sa

# ---------- Load Config ----------
with open("google_maps_config.yaml", "r") as file:
    config = yaml.safe_load(file)

place_parameter = config["place_parameter"]
channel_parameter = config["channel_parameter"]
google_maps_parameter = config["google_maps_parameter"]


# ---------- Set Variable ----------
google_maps_url = google_maps_parameter["url"]


# ---------- Set Browser ----------
def set_playwright_browser(p):
    browser = p.chromium.launch(
    headless=False,
    args=[
        "--disable-blink-features=AutomationControlled",
        "--no-sandbox",
        "--disable-infobars",
        "--disable-dev-shm-usage",
        "--disable-web-security",
        "--disable-extensions",
        "--disable-popup-blocking",
        "--disable-save-password-bubble"
    ])

    context = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36",
        viewport={"width": 1920, "height": 1080},
        java_script_enabled=True,
        ignore_https_errors=True,
    )
    page = context.new_page()

    return browser, context, page

# ---------- PHASE 1: Search and Extract Listings ----------
def extract_coords_from_url(url):
    try:
        match = re.search(r'!3d([-.\d]+)!4d([-.\d]+)', url)
        if match:
            lat = float(match.group(1))
            lon = float(match.group(2))
            return lat, lon
        else:
            print(f"⚠️ Coordinates not found in URL: {url}", flush=True)
            return None, None
    except Exception as e:
        print(f"❌ Error parsing coordinates: {e}", flush=True)
        return None, None


def search_and_extract(query_tuple):
    query, channel, district, state = query_tuple
    data = []

    with sync_playwright() as p:
        browser, context, page = set_playwright_browser(p)
        
        max_retries = 3
        search_success = False

        for attempt in range(max_retries):
            page.goto(google_maps_url, timeout=60000)
            time.sleep(2)
            try:
                search_box = page.wait_for_selector('xpath=//*[@id="searchboxinput"]', timeout=15000)
                print("search box found", flush=True)
                search_box.fill(query)
                search_box.press("Enter")
                page.wait_for_timeout(8000)
                print(f"entered {query} as query", flush=True)
                search_success = True
                break  # stop retrying
            except PlaywrightTimeoutError:
                print(f"⚠️ Attempt {attempt+1}: Search box not found, retrying...", flush=True)
                time.sleep(5)

        if not search_success:
            print("❌ Failed to enter query after retries — skipping.", flush=True)
            page.close()
            context.close()
            browser.close()
            return []

        try:
            scroll_container = page.wait_for_selector('//div[@role="feed"]', timeout=10000)
            
            if scroll_container:
                max_scrolls = 30
                scroll_count = 0

                while scroll_count < max_scrolls:
                    scroll_container.evaluate("el => el.scrollTop = el.scrollHeight")
                    print("scrolling...", flush=True)
                    time.sleep(2)
                    scroll_count += 1
                    elements = page.locator('div[class*="Nv2PK"][class*="THOPZb"]')
                    count = elements.count()

                    print(f"📦 Found {count} matching elements.", flush=True)

                    try:
                        # Check if end-of-list indicator is visible
                        no_more_container = page.wait_for_selector(".HlvSq", timeout=3000)
                        if no_more_container:
                            print("✅ End reached: Element with class 'HlvSq' found.", flush=True)
                            break
                    except PlaywrightTimeoutError:
                        pass  # Element not found yet, keep scrolling

                if scroll_count >= max_scrolls:
                    print(f"⚠️ Reached max scrolls ({max_scrolls}) — stopping.", flush=True)
                    
                    
        except Exception as e:
            print(f"❌ Error: {e}", flush=True)

        try:
            print("Trying to scrape container one by one...", flush=True)
            soup = bs(page.content(), 'html.parser')
            channel_container = soup.find_all("div", class_=lambda x: x and x.startswith("Nv2PK") and "THOPZb" in x)
            print(f'📦 Channel found: {len(channel_container)}', flush=True)

            for i, element in enumerate(channel_container):
                try:
                    a_tag = element.find("a", class_="hfpxzc")
                    href = a_tag.get("href") if a_tag else np.nan

                    name_tag = element.find("div", class_="qBF1Pd fontHeadlineSmall")
                    name = name_tag.text.strip() if name_tag else np.nan

                    rating_span = element.find("span", class_="MW4etd")
                    rating_text = rating_span.text.strip() if rating_span else ""
                    rating = float(rating_text) if rating_text.replace(".", "", 1).isdigit() else np.nan

                    # ✅ Fix category and address extraction
                    info_container = element.find("div", class_="UaQhfb fontBodyMedium")
                    if info_container:
                        info_container_2 = info_container.findAll("div", class_="W4Efsd")
                        if len(info_container_2) > 1:
                            info_container_3 = info_container_2[1].findAll("div", class_="W4Efsd")
                            if info_container_3:
                                spans = info_container_3[0].findAll("span")
                                filtered_spans = [s.text.strip() for s in spans if s.text.strip() and "·" not in s.text.strip()]
                                category = filtered_spans[0] if len(filtered_spans) >= 1 else np.nan
                                print(f'category: {category}')
                            else:
                                category= np.nan
                        else:
                            category = np.nan
                    else:
                        category = np.nan

                    latitude, longitude = None, None
                    if href:
                        latitude, longitude = extract_coords_from_url(href)

                    data.append({
                        "searched_query": query,
                        "searched_channel": channel,
                        "searched_district": district,
                        "searched_state": state,
                        "name": name,
                        "href": href,
                        "rating": rating,
                        "category": category,
                        "latitude": latitude, 
                        "longitude": longitude
                    })

                    print(f"✅ [{i+1}] Scraped: {name} | {href}", flush=True)

                    page.wait_for_timeout(2000) 

                except Exception as inner_e:
                    print(f"⚠️ Error scraping one item: {inner_e}", flush=True)

        except Exception as e:
            print(f"❌ Error getting element: {e}", flush=True)
        finally:
            page.close()
            context.close()
            browser.close()

    return data


# ---------- PHASE 2: Extract Location Details ----------
def extract_location_from_url(url):
    max_attempts = 3
    attempts = 0

    try:
        with sync_playwright() as p:
            
            browser, context, page = set_playwright_browser(p)

            phone_number = "-"
            address = "-"

            while attempts < max_attempts:
                try:
                    page.goto(url, timeout=60000)
                    print(f"🔍 Visiting: {url}", flush=True)
                    time.sleep(10)  # allow page load, adjust as needed

                    # Address extraction
                    try:
                        address_button = page.query_selector('//button[@data-item-id="address"]')
                        if address_button:
                            address_aria_label = address_button.get_attribute("aria-label") or "-"
                            if address_aria_label and "Address:" in address_aria_label:
                                    address = address_aria_label.replace("Address:", "").strip()
                            print(f"📫 Address found: {address}", flush=True)
                        else:
                            print(f"⚠️ Address button not found on attempt {attempts + 1}", flush=True)
                    except Exception as e:
                        print(f'⚠️ Address extraction error on attempt {attempts + 1}: {e}', flush=True)

                    # Phone number extraction
                    try:
                        buttons = page.query_selector_all('//button[@data-tooltip="Copy phone number"]')
                        for i, button in enumerate(buttons):
                            if button:
                                aria_label = button.get_attribute("aria-label")
                                if aria_label and "Phone:" in aria_label:
                                    phone_number = aria_label.replace("Phone:", "").strip()
                                    phone_number = str(phone_number).strip()
                                    phone_number = re.sub(r"[^+\d]", "", phone_number)

                                    print(f"📞 Phone number: {phone_number}", flush=True)
                                    break
                        if phone_number == "-":
                            print(f"⚠️ Phone number not found on attempt {attempts + 1}", flush=True)
                    except Exception as e:
                        print(f"⚠️ Phone number extraction error on attempt {attempts + 1}: {e}", flush=True)

                    # If address or phone found, we can break early
                    if address != "-" or phone_number != "-":
                        break

                except Exception as e:
                    print(f"[ERROR - Attempt {attempts + 1}] Failed scraping {url}: {e}", flush=True)

                attempts += 1

            page.close()
            context.close()
            browser.close()
            return (url, address, phone_number)

    except Exception as e:
        print(f"❌ Error extracting from {url}: {e}", flush=True)
        return (url, None, None)
    
def clean_address(address):
    if not isinstance(address, str):
        return address
    address = address.lower()
    address = re.sub(r',+', ',', address)
    address = re.sub(r'\s+', ' ', address)
    return address.strip()


def fuzzy_find(text, choices, threshold=85):
    """Helper function for fuzzy matching."""
    if not choices:
        return None
    result = process.extractOne(text, choices, scorer=fuzz.partial_ratio)
    if result:
        match, score, _ = result
        return match if score >= threshold else None
    return None


def extract_info_from_address(address, location_data, threshold=85):
    if not isinstance(address, str):
        return {"postcode": None, "state": None, "district": None, "area": None}

    address = clean_address(address)
    detected_postcode = re.findall(r"\d{5}", address)
    detected_postcode = detected_postcode[-1] if detected_postcode else None

    # Special case: Tioman
    if any(x in address for x in ["pulau tioman", "tioman island"]) or (
        "pahang" in address and "mersing" in address
    ):
        return {"postcode": "86800", "state": "pahang", "district": "rompin", "area": "pulau tioman"}

    # Direct postcode lookup
    if detected_postcode:
        for state, districts in location_data.items():
            for district, postcodes in districts.items():
                if detected_postcode in postcodes:
                    area = fuzzy_find(address, postcodes[detected_postcode]["locations"].get("area", []), threshold)
                    return {"postcode": detected_postcode, "state": state, "district": district, "area": area}

    # State → District → Postcode
    state = fuzzy_find(address, location_data.keys(), threshold)
    if state:
        district = fuzzy_find(address, location_data[state].keys(), threshold)
        if district:
            for pc, entry in location_data[state][district].items():
                area = fuzzy_find(address, entry["locations"].get("area", []), threshold)
                if (detected_postcode and detected_postcode == pc) or area:
                    return {"postcode": detected_postcode or pc, "state": state, "district": district, "area": area}
            return {"postcode": detected_postcode, "state": state, "district": district, "area": None}
        return {"postcode": detected_postcode, "state": state, "district": None, "area": None}

    # District → State → Postcode
    all_districts = {district: state for state, districts in location_data.items() for district in districts}
    district = fuzzy_find(address, all_districts.keys(), threshold)
    if district:
        state = all_districts[district]
        for pc, entry in location_data[state][district].items():
            area = fuzzy_find(address, entry["locations"].get("area", []), threshold)
            if (detected_postcode and detected_postcode == pc) or area:
                return {"postcode": detected_postcode or pc, "state": state, "district": district, "area": area}
        return {"postcode": detected_postcode, "state": state, "district": district, "area": None}

    # 4️⃣ No match at all
    return {"postcode": detected_postcode, "state": None, "district": None, "area": None}


def set_data_type(df):
    """
    Adjusts df to match Google Maps Redshift table:
    - Converts data types to match Redshift schema
    """
    column_types = {}
    for col_name in df.columns:
        if df[col_name].dtype == 'object':
            column_types[col_name] = sa.types.NVARCHAR(length=65535)
        elif df[col_name].dtype == 'datetime64[ns, UTC]':
            column_types[col_name] = sa.TIMESTAMP(timezone=True)
        elif df[col_name].dtype == 'float':
            column_types[col_name] = sa.FLOAT()
        elif col_name == 'extract_date':
            column_types[col_name] = sa.DATE()
    return column_types

def load_to_redshift(
        df,
        table_schema,
        table_name,
        rs_conn
    ):
    """
    Load dataframe to redshift
    """

    column_types = set_data_type(df)

    df.to_sql(
        table_name,
        rs_conn,
        index=False,
        dtype=column_types,
        if_exists='append',
        method='multi',
        schema=table_schema
    )


# # ---------- MAIN ENTRY ----------
if __name__ == "__main__":
    # # Phase 1 parallel
    query_jobs = []
    for channel_obj in channel_parameter:
        channel = channel_obj["channel"]
        for place in place_parameter:
            state = place["state"]
            for dist in place["districts"]:
                district = dist["district"]
                query = f"{channel} near {district}, {state}"
                query_jobs.append((query, channel, district, state))

    print(f"Running Phase 1 with {len(query_jobs)} queries using {cpu_count()} CPUs...")

    with Pool(processes=min(cpu_count(), 2)) as pool:
        results = pool.map(search_and_extract, query_jobs)

    # # Flatten results
    flat_data = [item for sublist in results for item in sublist]
    df_places = pd.DataFrame(flat_data)
    output_path = "data/phase1_results.csv"
    df_places.to_csv(output_path, index=False)
    print(f"✅ Saved to {output_path}")

    # # Phase 2 parallel
    df_places = pd.read_csv("data/phase1_results.csv")
    print(f"Running Phase 2 on {len(df_places)} URLs using {cpu_count()} CPUs...")
    hrefs = df_places["href"].dropna().unique().tolist()
    hrefs = hrefs[:10]

    with Pool(processes=min(cpu_count(), 8)) as pool:
        details = pool.map(extract_location_from_url, hrefs)

    df_coords = pd.DataFrame(details, columns=["href", "full_address", "phone_number"])
    df_final = df_places.merge(df_coords, on="href", how="left")
    df_final.to_csv("data/scraped_output.csv", index=False)

    print("✅ All done. Output saved to scraped_output.csv")

    # Phase 3 address preprocessing
    gm_df = pd.read_csv("data/scraped_output.csv")

    with open("state_district_postcode_location.json", "r") as f:
        location_data = json.load(f)

    extracted = gm_df["full_address"].apply(lambda x: extract_info_from_address(x, location_data))
    extracted_df = pd.json_normalize(extracted)
    gm_df = gm_df.join(extracted_df)

    gm_df.to_csv("data/gm_with_extracted_location.csv", index=False)










