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
from selenium.webdriver.common.action_chains import ActionChains
from utils import db_connection, setup_database, insert_jobs_batch
from dotenv import load_dotenv
import os
import undetected_chromedriver as uc

# Load environment variables from .env file
load_dotenv()

# Get LinkedIn credentials from environment variables
LINKEDIN_USERNAME = os.getenv("LINKEDIN_USERNAME")
LINKEDIN_PASSWORD = os.getenv("LINKEDIN_PASSWORD")

# Job categories to scrape
JOB_CATEGORIES = [
    "Quantitative Finance", "Quantitative Developer", "Quantitative Researcher", "Quantitative Analyst",
    "Consultant", "AI Research Scientist", "AI Scientist", "AI Engineer", "Data Scientist", "Data Engineer",
    "Machine Learning Scientist", "Machine Learning Engineer", "Energy", "Sustainability", "Environment", "Climate", "Education", "Lecturer"
]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_driver():
    options = uc.ChromeOptions()
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--start-maximized")
    return uc.Chrome(options=options)

def wait_for_element(driver, by, value, timeout=20):
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, value)))

def custom_wait_for_element(driver, by, value, timeout=20):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            element = driver.find_element(by, value)
            if element.is_displayed() and element.is_enabled():
                return element
        except NoSuchElementException:
            pass
        time.sleep(0.5)
    raise TimeoutException(f"Element not found: {by}={value}")

def safe_find_element(driver, by, value):
    try:
        return driver.find_element(by, value)
    except NoSuchElementException:
        return None

def human_like_typing(element, text):
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(0.1, 0.3))

def login_to_linkedin(driver, username, password):
    driver.get("https://www.linkedin.com/login")
    time.sleep(random.uniform(3, 5))
    
    username_field = custom_wait_for_element(driver, By.ID, "username")
    password_field = custom_wait_for_element(driver, By.ID, "password")
    
    human_like_typing(username_field, username)
    human_like_typing(password_field, password)
    
    login_button = custom_wait_for_element(driver, By.CSS_SELECTOR, "button[type='submit']")
    login_button.click()
    
    time.sleep(random.uniform(5, 7))

def scrape_job_listings(driver, conn, job_category, num_pages=10, max_retries=5):
    base_url = f"https://www.linkedin.com/jobs/search/?keywords={job_category.replace(' ', '%20')}&location=United%20Kingdom"
    new_jobs = []
    date_scraped = datetime.now().date()

    for page in range(1, num_pages + 1):
        url = f"{base_url}&start={25 * (page - 1)}" if page > 1 else base_url
        success = False
        retry_count = 0

        while not success and retry_count < max_retries:
            try:
                driver.get(url)
                time.sleep(random.uniform(5, 10))
                
                logging.info(f"Scraping {job_category} - page {page} (Attempt {retry_count + 1})")

                # Scroll down the page to load all job cards
                last_height = driver.execute_script("return document.body.scrollHeight")
                while True:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(random.uniform(1, 3))
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height

                job_cards = custom_wait_for_element(driver, By.CSS_SELECTOR, ".jobs-search__results-list")
                job_cards = job_cards.find_elements(By.TAG_NAME, "li")

                for card in job_cards:
                    try:
                        job = scrape_job_card(card, job_category, date_scraped, driver)
                        if job:
                            new_jobs.append(job)
                    except StaleElementReferenceException:
                        logging.warning("Stale element reference, skipping job card")
                    except Exception as e:
                        logging.error(f"Error scraping job card: {str(e)}")

                success = True
                logging.info(f"Completed {job_category} - page {page}, total new jobs scraped: {len(new_jobs)}")

            except TimeoutException:
                retry_count += 1
                logging.error(f"Timeout waiting for job cards on page {page}")
                time.sleep(60 * (2 ** retry_count))  # Exponential backoff
            except Exception as e:
                retry_count += 1
                logging.error(f"Unexpected error on page {page}: {str(e)}")
                time.sleep(60 * (2 ** retry_count))  # Exponential backoff

        if not success:
            logging.error(f"Failed to scrape {job_category} - page {page} after {max_retries} retries.")

    if new_jobs:
        insert_jobs_batch(conn, new_jobs)

def scrape_job_card(card, job_category, date_scraped, driver):
    try:
        title_elem = safe_find_element(card, By.CSS_SELECTOR, "h3.base-search-card__title")
        company_elem = safe_find_element(card, By.CSS_SELECTOR, "h4.base-search-card__subtitle")
        location_elem = safe_find_element(card, By.CSS_SELECTOR, "span.job-search-card__location")
        date_posted_elem = safe_find_element(card, By.CSS_SELECTOR, "time.job-search-card__listdate")
        url_elem = safe_find_element(card, By.CSS_SELECTOR, "a.base-card__full-link")

        job = {
            'source': 'LinkedIn',
            'category': job_category,
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

        if job['url'] != "N/A":
            job['description'], job['job_type'] = get_job_description(driver, job['url'])

        return (
            job['source'], job['category'], job['title'], job['company'], job['location'], job['salary'], 
            job['url'], job['description'], job['detailed_salary'], job['job_type'], job['detailed_location'], 
            job['date_posted'], job['date_scraped']
        )

    except StaleElementReferenceException:
        logging.warning("Stale element reference, skipping job card")
    except Exception as e:
        logging.error(f"Error scraping job card: {str(e)}")

    return None

def get_job_description(driver, url):
    original_window = driver.current_window_handle
    driver.execute_script("window.open('');")
    driver.switch_to.window(driver.window_handles[-1])
    driver.get(url)
    time.sleep(random.uniform(3, 5))

    try:
        description_elem = custom_wait_for_element(driver, By.CLASS_NAME, "show-more-less-html__markup")
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
    finally:
        driver.close()
        driver.switch_to.window(original_window)

    return description, job_type

def main():
    with db_connection() as conn:
        setup_database(conn)
        driver = setup_driver()
        try:
            login_to_linkedin(driver, LINKEDIN_USERNAME, LINKEDIN_PASSWORD)

            for job_category in JOB_CATEGORIES:
                logging.info(f"Starting to scrape for job category: {job_category}")
                scrape_job_listings(driver, conn, job_category)
                logging.info(f"Finished scraping for job category: {job_category}")

            logging.info("Scraping completed and data saved to database.")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {str(e)}")
        finally:
            driver.quit()

if __name__ == "__main__":
    main()