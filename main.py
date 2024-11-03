import requests
import re
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed


def extract_emails_from_html(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    emails = set(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",soup.text))
    return emails


def get_job_posting_urls(page_url):
    try:
        response = requests.get(page_url)
        response.raise_for_status()
        content = response.text
        soup = BeautifulSoup(content, "html.parser")
        post_divs = soup.find_all("div", class_="post-thumbnail")
        job_urls = [
            a["href"] for div in post_divs
            for a in div.find_all("a", href=True)
        ]
        return job_urls
    except requests.RequestException as e:
        print(f"An error occurred while fetching {page_url}: {e}")
        return []


def scrape_emails_from_website(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        content = response.text
        emails = extract_emails_from_html(content)
        return emails
    except requests.RequestException as e:
        print(f"An error occurred while fetching {url}: {e}")
        return set()


def scrape_page(page_num, base_url):
    page_url = f"{base_url}/page/{page_num}/"
    job_urls = get_job_posting_urls(page_url)
    all_emails = set()

    for job_url in job_urls:
        emails = scrape_emails_from_website(job_url)
        all_emails.update(emails)

    return all_emails


def scrape_all_emails(base_url, max_pages=300, max_workers=10):
    all_emails = set()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_page = {
            executor.submit(scrape_page, page_num, base_url): page_num
            for page_num in range(1, max_pages + 1)
        }

        for future in as_completed(future_to_page):
            page_num = future_to_page[future]
            try:
                emails = future.result()
                all_emails.update(emails)
                print(f"Found emails: {emails} on page {page_num}")
            except Exception as e:
                print(
                    f"An error occurred while processing page {page_num}: {e}")

    return all_emails


base_url = "https://oilgasvacancies.com"
emails = scrape_all_emails(base_url, max_pages=300, max_workers=10)
if emails:
    with open("emails.txt", "w") as file:
        for email in emails:
            file.write(email + "\n")
    print("Emails have been saved to emails.txt")
else:
    print("No emails found.")
