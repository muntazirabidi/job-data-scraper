import time
import random
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime
from utils import db_connection, setup_database, insert_jobs_batch, retry_operation

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Job categories to scrape
JOB_CATEGORIES = [
    "Quantitative Finance", "Quantitative Developer", "Quantitative Researcher", "Quantitative Analyst",
    "Consultant", "AI Research Scientist", "AI Scientist", "AI Engineer", "Data Scientist", "Data Engineer",
    "Machine Learning Scientist", "Machine Learning Engineer", "Energy"
]

def setup_driver(user_agent=None):
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    if user_agent:
        options.add_argument(f'user-agent={user_agent}')
    else:
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    options.add_argument('--headless')  # Optional: use headless mode for performance
    service = Service('/usr/local/bin/chromedriver')  # Adjust path as needed
    return webdriver.Chrome(service=service, options=options)

def wait_for_element(driver, by, value, timeout=20):
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, value)))

def safe_find_element(driver, by, value):
    try:
        return driver.find_element(by, value)
    except NoSuchElementException:
        return None

def scrape_job_listings(driver, job_category, num_pages=20):
    base_url = f"https://uk.indeed.com/jobs?q={job_category.replace(' ', '+')}&l=United+Kingdom"
    all_jobs = []
    date_scraped = datetime.now().date()

    for page in range(num_pages):
        url = f"{base_url}&start={page*10}" if page > 0 else base_url
        driver.get(url)
        time.sleep(random.uniform(3, 5))

        logging.info(f"Scraping {job_category} - page {page + 1}")

        try:
            job_cards = wait_for_element(driver, By.CSS_SELECTOR, 'div.job_seen_beacon')
            job_cards = driver.find_elements(By.CSS_SELECTOR, 'div.job_seen_beacon')
        except TimeoutException:
            logging.error(f"Timeout waiting for job cards on page {page + 1}")
            continue

        for card in job_cards:
            try:
                title_elem = safe_find_element(card, By.CSS_SELECTOR, 'h2.jobTitle span')
                company_elem = safe_find_element(card, By.CSS_SELECTOR, 'span[data-testid="company-name"]')
                location_elem = safe_find_element(card, By.CSS_SELECTOR, 'div[data-testid="text-location"]')
                salary_elem = safe_find_element(card, By.CSS_SELECTOR, 'div[class*="salary-snippet"]')
                url_elem = safe_find_element(card, By.CSS_SELECTOR, 'h2.jobTitle a')

                job = {
                    'source': 'Indeed',
                    'category': job_category,
                    'title': title_elem.text if title_elem else "N/A",
                    'company': company_elem.text if company_elem else "N/A",
                    'location': location_elem.text if location_elem else "N/A",
                    'salary': salary_elem.text if salary_elem else "Not provided",
                    'url': url_elem.get_attribute('href') if url_elem else "N/A",
                    'description': "To be fetched",
                    'detailed_salary': None,
                    'job_type': None,
                    'detailed_location': None,
                    'date_posted': None,  # Indeed doesn't always provide date posted easily
                    'date_scraped': date_scraped
                }

                all_jobs.append(job)

            except StaleElementReferenceException:
                logging.warning("Stale element reference, skipping job card")
            except Exception as e:
                logging.error(f"Error scraping job card: {str(e)}")

        logging.info(f"Completed {job_category} - page {page + 1}, total jobs scraped: {len(all_jobs)}")

    return all_jobs

def get_job_description(driver, job):
    driver.get(job['url'])
    time.sleep(random.uniform(2, 4))

    try:
        description = wait_for_element(driver, By.ID, 'jobDescriptionText').text
        
        salary = safe_find_element(driver, By.CSS_SELECTOR, 'span[class*="css-19j1a75"]')
        job_type = safe_find_element(driver, By.CSS_SELECTOR, 'span[class*="css-k5flys"]')
        location = safe_find_element(driver, By.ID, 'jobLocationText')

        job.update({
            'description': description,
            'detailed_salary': salary.text if salary else "Not provided",
            'job_type': job_type.text if job_type else "Not specified",
            'detailed_location': location.text if location else "Not specified"
        })
    except TimeoutException:
        logging.warning(f"Timeout while fetching job description for {job['url']}")
        job.update({
            'description': "Description not available",
            'detailed_salary': "Not provided",
            'job_type': "Not specified",
            'detailed_location': "Not specified"
        })

    return job

def scrape_category(job_category):
    driver = setup_driver()
    try:
        jobs = scrape_job_listings(driver, job_category)
        detailed_jobs = []
        for job in jobs:
            detailed_job = retry_operation(lambda: get_job_description(driver, job), retries=3)
            detailed_jobs.append(detailed_job)
            time.sleep(random.uniform(1, 3))
        return detailed_jobs
    finally:
        driver.quit()

def main():
    with db_connection() as conn:
        setup_database(conn)

        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_category = {executor.submit(scrape_category, category): category for category in JOB_CATEGORIES}
            for future in as_completed(future_to_category):
                category = future_to_category[future]
                try:
                    jobs = future.result()
                    insert_jobs_batch(conn, jobs)
                    logging.info(f"Completed scraping for {category}. Total jobs: {len(jobs)}")
                except Exception as exc:
                    logging.error(f'{category} generated an exception: {exc}')
    
    logging.info("Job scraping completed. Data saved to SQLite database.")

if __name__ == "__main__":
    main()
