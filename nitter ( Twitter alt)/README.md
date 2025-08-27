# Nitter Scraper

This project scrapes data from [Nitter](https://nitter.net), a free and open source alternative Twitter front-end.  
It allows you to extract tweets, user details, and related metadata for analysis without using the official Twitter API.

---

## 🚀 Features
- Scrape tweets from Nitter profiles or search results
- Extract tweet text, date, retweets, likes, and replies
- Save results to CSV/JSON for further processing
- Lightweight and avoids Twitter API restrictions

---

## 📦 Requirements
Install the dependencies with:

```bash
pip install -r requirements.txt


📝 Notes

Nitter instances may block frequent scraping, so adjust request intervals if needed.

If the main nitter.net is down, try a public Nitter instance