import time
import random
import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from utils import db_connection, setup_database, insert_jobs_batch

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    service = Service('/usr/local/bin/chromedriver')  # Adjust path as needed
    return webdriver.Chrome(service=service, options=options)

def wait_for_element(driver, by, value, timeout=20):
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, value)))

def safe_find_element(driver, by, value):
    try:
        return driver.find_element(by, value)
    except NoSuchElementException:
        return None

def login_to_linkedin(driver, username, password):
    driver.get("https://www.linkedin.com/login")
    time.sleep(3)
    
    username_field = wait_for_element(driver, By.ID, "username")
    password_field = wait_for_element(driver, By.ID, "password")
    
    username_field.send_keys(username)
    password_field.send_keys(password)
    
    login_button = wait_for_element(driver, By.CSS_SELECTOR, "button[type='submit']")
    login_button.click()
    
    time.sleep(5)  # Wait for login to complete

def scrape_job_listings(driver, conn, num_pages=10):
    base_url = "https://www.linkedin.com/jobs/search/?keywords=Data%20Scientist&location=United%20Kingdom"
    new_jobs = []
    date_scraped = datetime.now().date()

    for page in range(1, num_pages + 1):
        url = f"{base_url}&start={25 * (page - 1)}" if page > 1 else base_url
        driver.get(url)
        time.sleep(random.uniform(3, 7))

        logging.info(f"Scraping page {page}")

        try:
            job_cards = wait_for_element(driver, By.CLASS_NAME, "jobs-search__results-list")
            job_cards = job_cards.find_elements(By.TAG_NAME, "li")

            for card in job_cards:
                try:
                    title_elem = safe_find_element(card, By.CSS_SELECTOR, "h3.base-search-card__title")
                    company_elem = safe_find_element(card, By.CSS_SELECTOR, "h4.base-search-card__subtitle")
                    location_elem = safe_find_element(card, By.CSS_SELECTOR, "span.job-search-card__location")
                    date_posted_elem = safe_find_element(card, By.CSS_SELECTOR, "time.job-search-card__listdate")
                    url_elem = safe_find_element(card, By.CSS_SELECTOR, "a.base-card__full-link")

                    job = {
                        'source': 'LinkedIn',
                        'category': None,
                        'title': title_elem.text if title_elem else "N/A",
                        'company': company_elem.text if company_elem else "N/A",
                        'location': location_elem.text if location_elem else "N/A",
                        'salary': None,
                        'url': url_elem.get_attribute('href') if url_elem else "N/A",
                        'description': "To be fetched",
                        'detailed_salary': None,
                        'job_type': "Not specified",
                        'detailed_location': None,
                        'date_posted': date_posted_elem.get_attribute('datetime') if date_posted_elem else "N/A",
                        'date_scraped': date_scraped
                    }

                    # Fetch full job description
                    if job['url'] != "N/A":
                        job['description'], job['job_type'] = get_job_description(driver, job['url'])

                    new_jobs.append((
                        job['source'], job['category'], job['title'], job['company'], job['location'], job['salary'], 
                        job['url'], job['description'], job['detailed_salary'], job['job_type'], job['detailed_location'], 
                        job['date_posted'], job['date_scraped']
                    ))

                    logging.info(f"Scraped: {job['title']} at {job['company']}")

                except StaleElementReferenceException:
                    logging.warning("Stale element reference, skipping job card")
                except Exception as e:
                    logging.error(f"Error scraping job card: {str(e)}")

        except TimeoutException:
            logging.error(f"Timeout waiting for job cards on page {page}")
            continue

        logging.info(f"Completed page {page}, total new jobs scraped: {len(new_jobs)}")

    if new_jobs:
        insert_jobs_batch(conn, new_jobs)

def get_job_description(driver, url):
    original_window = driver.current_window_handle
    driver.execute_script("window.open('');")
    driver.switch_to.window(driver.window_handles[-1])
    driver.get(url)
    time.sleep(random.uniform(2, 4))

    try:
        description_elem = wait_for_element(driver, By.CLASS_NAME, "show-more-less-html__markup")
        description = description_elem.text

        job_type_elem = safe_find_element(driver, By.CSS_SELECTOR, "li.description__job-criteria-item:nth-child(2) span")
        job_type = job_type_elem.text if job_type_elem else "Not specified"
    except TimeoutException:
        description = "Description not available"
        job_type = "Not specified"
    except Exception as e:
        logging.error(f"Error fetching job description: {str(e)}")
        description = "Error fetching description"
        job_type = "Not specified"

    driver.close()
    driver.switch_to.window(original_window)
    return description, job_type

def main():
    with db_connection() as conn:
        setup_database(conn)
        driver = setup_driver()
        try:
            # Provide LinkedIn credentials here
            login_to_linkedin(driver, "your_username", "your_password")
            scrape_job_listings(driver, conn)
            logging.info("Scraping completed and data saved to database.")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {str(e)}")
        finally:
            driver.quit()

if __name__ == "__main__":
    main()
