Shamelessly written, using ChatGPT o3-mini-high, by JR

How to Run

    Save the script as scrape_site.py.

    Make it executable:

chmod +x scrape_site.py

Install the dependency (if not already installed):

pip install markdownify

Launch the script from the terminal. For example, to scrape
https://docs.cribl.io/stream/4.7/upgrading-workers
and save files under ./Stream, run:

    ./scrape_site.py https://docs.cribl.io/stream/4.7/upgrading-workers ./Stream --max_depth 2 --rate_limit 1.5 --max_concurrency 3

This configuration ensures that files are stored under a folder that begins with the host name, followed by the URL path, and the final file name reflects the last endpoint (with any fragment appended using an underscore), so that files do not get overwritten across different versions.
