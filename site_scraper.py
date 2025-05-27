#!/usr/bin/env python3
import os
import argparse
import requests
import time
import ray
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
from markdownify import markdownify as md

@ray.remote
class RateLimiter:
    def __init__(self, delay):
        self.delay = delay
        self.last_request = 0.0

    def acquire(self):
        now = time.time()
        if self.last_request == 0.0:
            self.last_request = now
            return
        elapsed = now - self.last_request
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_request = time.time()

@ray.remote
def scrape_page(url, rate_limiter=None):
    """
    Fetch a URL and return a dictionary with:
      - url: the page URL,
      - markdown: the page content converted to Markdown (with hyperlinks, headers, bold, italics, images preserved),
      - links: a list of hyperlinks (for crawling).
    """
    try:
        if rate_limiter is not None:
            ray.get(rate_limiter.acquire.remote())
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        # Convert HTML to Markdown (using ATX-style headers)
        markdown_content = md(response.text, heading_style="ATX")
        # Extract links using BeautifulSoup for crawling
        soup = BeautifulSoup(response.text, 'html.parser')
        links = [a['href'] for a in soup.find_all('a', href=True)]
        return {'url': url, 'markdown': markdown_content, 'links': links}
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return {'url': url, 'markdown': '', 'links': []}

def filter_links(links, base_url):
    """Convert links to absolute URLs and filter to only include those on the same domain as base_url."""
    filtered = []
    base_domain = urlparse(base_url).netloc
    for link in links:
        absolute = urljoin(base_url, link)
        if urlparse(absolute).netloc == base_domain:
            filtered.append(absolute)
    return filtered

def sanitize_filename(name):
    """Sanitize a string to be safe for file and directory names."""
    return re.sub(r'[\\/*?:"<>|]', "_", name)

def save_page(url, markdown_content, output_dir):
    """
    Save the scraped page as a Markdown (.md) file.
    
    The full saved path is built as follows:
      - First folder: the host (e.g. "docs.cribl.io")
      - Then the URL path parts.
      - The file name is based on the last path element.
         • If a fragment is present, it is appended with an underscore.
    
    For example:
      - For URL: https://docs.cribl.io/stream/4.7/upgrading-workers  
        → saved as: <output_dir>/docs.cribl.io/stream/4.7/upgrading-workers.md
      - For URL: https://docs.cribl.io/stream/deploy-cloud#pricing  
        → saved as: <output_dir>/docs.cribl.io/stream/deploy-cloud/deploy-cloud_pricing.md
    """
    parsed = urlparse(url)
    host = sanitize_filename(parsed.netloc)
    path = parsed.path
    fragment = parsed.fragment

    # Break the path into sanitized parts (ignoring empty parts)
    path_parts = [sanitize_filename(part) for part in path.split('/') if part]

    if path_parts:
        final_endpoint = path_parts[-1]
        dir_parts = path_parts[:-1]
    else:
        # If no path, use the host as the final endpoint.
        final_endpoint = host
        dir_parts = []

    # Build the directory structure: output_dir / host / (dir_parts)
    directory = os.path.join(output_dir, host, *dir_parts)
    
    # Build the file name: final_endpoint.md, with fragment appended if present.
    if fragment:
        filename = f"{final_endpoint}_{sanitize_filename(fragment)}.md"
    else:
        filename = f"{final_endpoint}.md"

    os.makedirs(directory, exist_ok=True)
    file_path = os.path.join(directory, filename)
    
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(f"# {url}\n\n{markdown_content}")
    print(f"Saved {url} to {file_path}")

def process_urls(urls, rate_limiter, max_concurrency):
    """
    Process a list of URLs with limited concurrency.
    Launch tasks in batches up to max_concurrency.
    """
    tasks = []
    results = []
    for url in urls:
        tasks.append(scrape_page.remote(url, rate_limiter))
        if len(tasks) >= max_concurrency:
            done, tasks = ray.wait(tasks, num_returns=1)
            results.extend(ray.get(done))
    if tasks:
        results.extend(ray.get(tasks))
    return results

def crawl_website(base_url, output_dir, max_depth=1, rate_limiter=None, max_concurrency=4):
    """
    Crawl the website from base_url up to max_depth.
    Saves each page as Markdown with a directory structure matching the URL.
    """
    visited = set()
    current_level = [base_url]
    depth = 0

    while current_level and depth <= max_depth:
        print(f"Crawling depth {depth} with {len(current_level)} page(s)...")
        urls_to_scrape = [url for url in current_level if url not in visited]
        results = process_urls(urls_to_scrape, rate_limiter, max_concurrency)
        next_level = []
        for result in results:
            url = result['url']
            if url in visited:
                continue
            visited.add(url)
            save_page(url, result['markdown'], output_dir)
            new_links = filter_links(result['links'], base_url)
            for link in new_links:
                if link not in visited and link not in current_level and link not in next_level:
                    next_level.append(link)
        current_level = next_level
        depth += 1

def main():
    parser = argparse.ArgumentParser(
        description="Scrape a website and save pages as Markdown with formatting preserved."
    )
    parser.add_argument("site", help="Base URL to scrape (e.g., https://docs.cribl.io/stream/4.7/upgrading-workers)")
    parser.add_argument("output_dir", help="Directory to save the Markdown files")
    parser.add_argument("--max_depth", type=int, default=1, help="Maximum crawl depth (default: 1)")
    parser.add_argument("--rate_limit", type=float, default=0.0,
                        help="Delay in seconds between requests (default: 0, no rate limiting)")
    parser.add_argument("--max_concurrency", type=int, default=4,
                        help="Maximum number of concurrent requests (default: 4)")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    ray.init(ignore_reinit_error=True)
    rate_limiter = None
    if args.rate_limit > 0:
        rate_limiter = RateLimiter.remote(args.rate_limit)

    crawl_website(
        base_url=args.site,
        output_dir=args.output_dir,
        max_depth=args.max_depth,
        rate_limiter=rate_limiter,
        max_concurrency=args.max_concurrency
    )

if __name__ == "__main__":
    main()
