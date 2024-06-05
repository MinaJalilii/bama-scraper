import requests_html


def scrape_bama_data(url):
    try:
        session = requests_html.HTMLSession()
        encountered_ads = []
        repeated_ads_count = 0
        j = 0
        while True:
            try:
                r = session.get(f'{url}?pageIndex={j}')
            except requests_html.HTTPError as e:
                print(f"HTTP error occurred: {e}")
            r.raise_for_status()
            js = r.json()
            ads = js['data']['ads']
            if not ads:
                break
            for i in ads:
                if i['type'] == 'ad':
                    code = i['detail']['code']
                    if code in encountered_ads:
                        repeated_ads_count += 1
                        # If the count reaches 36, stop scraping
                        if repeated_ads_count >= 36:
                            break
                    else:
                        repeated_ads_count = 0
                        encountered_ads.append(code)
                else:
                    continue
            j += 1
    except requests_html.RequestException as e:
        print(f"Request error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


bama_url = 'https://bama.ir/cad/api/search'
scrape_bama_data(bama_url)
