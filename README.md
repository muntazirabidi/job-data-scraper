# Data Scraping for Job Listings

## Overview
This project contains Python scripts for scraping job listings from LinkedIn and Indeed. The data is stored in a unified SQLite database (`job_listings.db`).

## Folder Structure
'''
project-root/
    ├── data-scraping/           # Folder for scraping scripts and utilities
    │   ├── linkedin_scraper.py  # Script for LinkedIn scraping
    │   ├── indeed_scraper.py    # Script for Indeed scraping
    │   ├── utils.py             # Utility functions (e.g., database connection, insertion)
    │   ├── job_listings.db      # SQLite database (initially empty, will be populated by scrapers)
    │   └── requirements.txt     # Python dependencies for scraping
    └── README.md                # Documentation
'''


## Installation

1. Clone the repository.
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```


Ensure that you have ChromeDriver installed and available in your system PATH.

## Usage

### LinkedIn Scraper
1. Update your LinkedIn credentials in the linkedin_scraper.py file.
2. Run the LinkedIn scraper:
```bash
python linkedin_scraper.py

```
### Indeed Scraper
1. Run the Indeed scraper
```bash
python indeed_scraper.py
```

Database

The job listings are stored in the job_listings.db SQLite database. Both LinkedIn and Indeed data are stored in the same jobs table, differentiated by the source column.


---

### **Conclusion**
You now have a well-structured project for scraping job listings from LinkedIn and Indeed. The scraping scripts store data in a unified SQLite database, and the `utils.py` file provides shared utility functions for database operations.

You can easily run the scrapers, and both LinkedIn and Indeed job data will be stored in the same database for further analysis or integration with other applications.

Would you like further assistance with setting up or customizing the scrapers?
