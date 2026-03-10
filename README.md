# Scraper - Haeusermann + Partner

## Overview
Scrapes job listings from Haeusermann + Partner. Extracts job descriptions from PDF files.

- **Website:** https://haeusermann.ch/de/firma/karriere/
- **Industry:** Rechtsberatung und Notariat

## Jobs Scraped
- Notar (w/m/d)
- Sachbearbeiter Notariat
- Notariatskandidaten - Bern
- Notariatskandidaten - Fraubrunnen

## Features
- Scrapes job listings directly from company website
- Parses structured data using LLM (Groq API)
- Saves directly to MySQL database
- Skips already processed jobs (no duplicates)
- Auto-retry on API quota limits
- Uses UUID for unique job identification

## Tech Stack
- Python 3
- Scrapy / Requests
- MySQL
- Groq API (LLM)
- pypdf (for PDF extraction)

## Project Structure
- main.py - Main scraper script
- model.py - Database models and helper functions
- utils.py - LLM utility functions
- requirements.txt - Python dependencies
- .env - Environment variables (not included)

## Setup

1. Clone the repository:
   git clone https://github.com/zahid2021/scraper-haeusermann.git

2. Install dependencies:
   pip install -r requirements.txt

3. Create .env file:
   DB_HOST=localhost
   DB_USER=your_db_user
   DB_PASSWORD=your_db_password
   DB_DATABASE=job_portal_db
   GROQ_API_KEY=your_groq_api_key

4. Run the scraper:
   python3 main.py

## How It Works
1. Connects to MySQL database
2. Loads already processed job links to avoid duplicates
3. Scrapes job listings from website
4. Extracts job descriptions
5. Parses structured data using LLM
6. Saves to database

## Notes
- .env file is required but not included for security reasons
- Script automatically stops if API quota is exceeded
- Duplicate jobs are skipped automatically
