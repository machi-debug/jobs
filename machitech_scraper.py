import requests
from bs4 import BeautifulSoup
from lxml import etree

def get_job_urls():
    """Finds all live job and emploi URLs from the sitemap."""
    sitemap_url = "https://machitech.com/sitemap.xml"
    response = requests.get(sitemap_url)
    soup = BeautifulSoup(response.content, 'xml')
    
    # Extract all links that belong to the jobs or emplois sections
    all_links = [loc.text for loc in soup.find_all("loc")]
    job_links = [link for link in all_links if "/jobs/" in link or "/emplois/" in link]
    
    # Remove the main listing pages so we only get individual job posts
    exclude = ["https://machitech.com/jobs", "https://machitech.com/emplois"]
    return [link for link in job_links if link.strip("/") not in exclude]

def build_linkedin_xml(urls):
    source = etree.Element("source")
    etree.SubElement(source, "publisher").text = "Machitech"
    etree.SubElement(source, "publisherurl").text = "https://machitech.com"

    for url in urls:
        print(f"Scraping: {url}")
        res = requests.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        job = etree.SubElement(source, "job")
        
        # Title & ID
        title = soup.find("h1").text.strip() if soup.find("h1") else "Machitech Job"
        job_id = url.split("/")[-1] # Uses the URL slug as the ID
        
        etree.SubElement(job, "title").text = etree.CDATA(title)
        etree.SubElement(job, "referencenumber").text = etree.CDATA(job_id)
        etree.SubElement(job, "url").text = etree.CDATA(url)
        etree.SubElement(job, "company").text = etree.CDATA("Machitech")

        # Description (Scrapes the main content area)
        # You may need to adjust the class name '.job-description' based on your Hubspot module
        desc_div = soup.select_one('.job-description, .body-container, article')
        description = desc_div.text.strip() if desc_div else "See website for details."
        etree.SubElement(job, "description").text = etree.CDATA(description)

        # Location Logic
        is_french = "/emplois/" in url
        etree.SubElement(job, "city").text = "Victoriaville" if is_french else "Livermore"
        etree.SubElement(job, "state").text = "Québec" if is_french else "KY"
        etree.SubElement(job, "country").text = "CA" if is_french else "US"
        etree.SubElement(job, "jobtype").text = etree.CDATA("Full time")

    # Save the file
    with open("linkedin-jobs.xml", "wb") as f:
        f.write(etree.tostring(source, pretty_print=True, xml_declaration=True, encoding="UTF-8"))

# Run the automation
found_urls = get_job_urls()
build_linkedin_xml(found_urls)