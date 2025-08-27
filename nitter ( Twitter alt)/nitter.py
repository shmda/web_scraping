from selenium import webdriver 
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait as wdw
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from datetime import datetime
import pandas as pd
import time 
import json
import os
import random

# Example cookies for session behavior
cookies = {
    'infiniteScroll': 'on',
    'hlsPlayback': 'on',
}

# Minimal headers (user agent only)
headers = {
    'user-agent': 'Mozilla/5.0 (compatible; WebScraper/1.0)',
}

def driver_init():
    """Initialize Selenium WebDriver with basic options."""
    firefox_options = Options()
    firefox_options.add_argument("--incognito")
    firefox_options.add_argument("--ignore-certificate-errors")
    firefox_options.add_argument("--disable-dev-shm-usage")
    firefox_options.add_argument("--no-sandbox")
    firefox_options.add_argument("--start-maximized")
	
    return webdriver.Chrome(options=firefox_options)


def scrape_nitter(url, keyword, tweets):
    """
    Scrape tweets from a given Nitter instance URL for a specific keyword.
    """

    print(f"Scraping keyword '{keyword}' from {url}")
    
    driver = driver_init()
    driver.get(url)

    for name, value in cookies.items():
        driver.add_cookie({'name': name, 'value': value})

    driver.refresh()

    timeline_container = wdw(driver, 20).until(
        EC.presence_of_element_located((By.CLASS_NAME, 'timeline'))
    )

    prev_count = 0
    max_wait_time = 300  
    max_no_new_content_time = 60  
    start_time = time.time()

    while True:
        driver.execute_script("arguments[0].scrollIntoView({block: 'end'});", timeline_container)
        time.sleep(2)

        tweets_list = timeline_container.find_elements(By.CLASS_NAME, 'timeline-item')
        current_count = len(tweets_list)
        print(f"Tweets found: {current_count}")

        show_more_button = driver.find_elements(By.CLASS_NAME, "show-more")

        if show_more_button:
            print("Loading more content...")
            wait_start_time = time.time()

            while True:
                time.sleep(10)
                driver.execute_script("arguments[0].scrollIntoView({block: 'end'});", timeline_container)
                tweets_list = timeline_container.find_elements(By.CLASS_NAME, 'timeline-item')
                new_count = len(tweets_list)

                if new_count > current_count:
                    print("New content loaded!")
                    prev_count = new_count
                    break  

                if time.time() - wait_start_time > max_no_new_content_time:
                    print("No new content after waiting. Stopping...")
                    exit()

        else:
            print("No more content to load. Stopping...")
            break

        if time.time() - start_time > max_wait_time:
            print("Total time exceeded. Stopping...")
            break

        time.sleep(3)

    # Extract tweets
    tweets_list = timeline_container.find_elements(By.CLASS_NAME, 'timeline-item')
    print(f'Total tweets extracted: {len(tweets_list)}')

    for tweet in tweets_list:
        try:
            tweet_link = tweet.find_element(By.CLASS_NAME,'tweet-link').get_attribute("href")
            tweet_username_fullname = tweet.find_element(By.CLASS_NAME, 'fullname-and-username')
            tweet_fullname = tweet_username_fullname.find_element(By.CLASS_NAME, 'fullname').text.strip()
            tweet_username = tweet_username_fullname.find_element(By.CLASS_NAME, 'username').text.strip()

            tweet_date_element = tweet.find_element(By.CLASS_NAME, 'tweet-date')
            date_anchor = tweet_date_element.find_element(By.TAG_NAME, 'a')
            timestamp_title = date_anchor.get_attribute('title')

            if timestamp_title:
                clean_timestamp = timestamp_title.replace(' ·', '').replace(' UTC', '')
                dt = datetime.strptime(clean_timestamp, '%b %d, %Y %I:%M %p')
                tweet_timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                tweet_timestamp = None

            tweet_content = tweet.find_element(By.CLASS_NAME, 'tweet-content.media-body')
            text = tweet_content.text.strip() if tweet_content else None

            # Extract images
            img_src_list = [
                img.get_attribute('src')
                for img in tweet.find_elements(By.CSS_SELECTOR, 'div.attachments .attachment.image a.still-image img')
            ]

            # Extract videos
            vid_src_list = [
                vid.get_attribute('data-url')
                for vid in tweet.find_elements(By.CSS_SELECTOR, 'div.attachments.card .gallery-video .attachment.video-container video')
            ]

            # Extract stats
            comment_count = retweet_count = quote_count = heart_count = 0
            try:
                tweet_stats = tweet.find_element(By.CLASS_NAME, 'tweet-stats')
                stats = tweet_stats.find_elements(By.CLASS_NAME, 'tweet-stat')
                if len(stats) >= 4:
                    comment_count = stats[0].text.strip() or '0'
                    retweet_count = stats[1].text.strip() or '0'
                    quote_count = stats[2].text.strip() or '0'
                    heart_count = stats[3].text.strip() or '0'
            except NoSuchElementException:
                pass

            extract_datetime = time.strftime('%Y-%m-%d %H:%M:%S')  
            extract_date = time.strftime('%Y-%m-%d')
            
            tweets_temp = pd.DataFrame({
                'keyword': [keyword],
                'tweet_link': [tweet_link],
                'tweet_username': [tweet_username],
                'tweet_fullname': [tweet_fullname],
                'tweet_timestamp': [tweet_timestamp],
                'tweet': [text],
                'img_src': [json.dumps(img_src_list)], 
                'vid_src': [json.dumps(vid_src_list)], 
                'comment_count': [comment_count],
                'retweet_count': [retweet_count],
                'quote_count': [quote_count],
                'heart_count': [heart_count],
                'extract_datetime': [extract_datetime],
                'extract_date': [extract_date]
            })

            tweets = pd.concat([tweets, tweets_temp], ignore_index=True)
            time.sleep(random.uniform(3, 7))

        except Exception as e:
            print(f"Error extracting tweet: {e}")
            continue

    driver.quit()
    return tweets
        

def save_to_csv(all_tweets):
    """Save tweets to Excel file."""
    extract_date = time.strftime('%Y-%m-%d')
    file_path = f"Result/tweet_data_{extract_date}.xlsx"
    sheet_name = "tweet_data"

    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    all_tweets.to_excel(file_path, sheet_name=sheet_name, index=False)

    print(f"Created new file: {file_path} with sheet '{sheet_name}'")


def main():
    """Example main function (modify with your own Nitter instance and keyword)."""
    all_tweets = pd.DataFrame()
    url = "https://nitter.net/search?q=example"
    keyword = "example"
    all_tweets = scrape_nitter(url, keyword, all_tweets)
    save_to_csv(all_tweets)


if __name__ == '__main__':
    main()
