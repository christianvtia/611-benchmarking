import threading
import queue
import time
import os

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlsplit, urlunsplit

from PageData import PageData
from typing import Optional

import tldextract
import csv
from datetime import datetime

import Pyro5.api
from Pyro5.api import expose, serve, config, register_class_to_dict, register_dict_to_class

ip = "10.2.13.30" # "192.168.1.81"
seed = "https://www.dlsu.edu.ph"
runTimeMinutes = 0.5
nScrapers = 9
nExtractors = 1

#how many seconds per record
recordTimeInterval = 15

def parseLabelFromURL(url):
    parts = urlsplit(url)
    path = parts.path.strip('/')
    
    #for 'single-part' URLs like 'https://enroll.dlsu.edu.ph'
    if not path:
        return f"{url} [NO LABEL]"
    
    
    #replace separators with spaces and capitalize each word
    label = path.replace('/', ' ').replace('-', ' ').replace('_', ' ')
    label = ' '.join(word.capitalize() for word in label.split())
    
    return label

@Pyro5.api.expose
class Database:
    def __init__(self):
        self.seenURLs = set()
        self.unsearchedURLs = queue.Queue()
        self.unextractedPages = queue.Queue()
        self.numFinishedPages = 0

        #url acts as key, label is value
        self.gatheredURLs = {}

        #for seen urls
        self.seenLock = threading.Lock()
        
        #for num finished pages
        self.finishedPagesLock = threading.Lock()

        #for gatheredurls
        self.finishedURLLock = threading.Lock()
        
        self.shutdownFlag = threading.Event()
        
        #for tracking seen URLs over time
        self.seenURLsOverTime = []
        self.unsearchedSizeOverTime = []
        self.unextractedSizeOverTime = []
        
        #moved from globals
        self.domain = ""
        self.isHttpHttpsDistinct = False

        self.blacklistedHosts = ["animorepository.dlsu.edu.ph"]
    
    def setDomain(self, url):
        extracted = tldextract.extract(url)
        self.domain = f"{extracted.domain}.{extracted.suffix}"
        return self.domain
    
    def isSameDomain(self, url):
        if self.domain is None:
            return True
        extracted = tldextract.extract(url)
        urlDomain = f"{extracted.domain}.{extracted.suffix}"
        return urlDomain == self.domain
    
    def isShutdown(self):
        return self.shutdownFlag.is_set()

    def isBlacklisted(self, url):
        return urlsplit(url).netloc.lower() in self.blacklistedHosts
    
    def getSeenCount(self):
        with self.seenLock:
            return len(self.seenURLs)
    
    def pushUnsearched(self, url):
        self.unsearchedURLs.put(url)
    
    def popUnsearched(self, timeout=1):
        try:
            return self.unsearchedURLs.get(timeout=timeout)
        except queue.Empty:
            return None
    
    def pushUnextractedPage(self, pageData):
        self.unextractedPages.put(pageData)
    
    def popUnextractedPage(self, timeout=1):
        try:
            return self.unextractedPages.get(timeout=timeout)
        except queue.Empty:
            return None
    
    def addToSeen(self, url):
        with self.seenLock:
            #normalize URL for comparison if http/https are not distinct
            compareUrl = url
            if not self.isHttpHttpsDistinct:
                parts = urlsplit(url)
                compareUrl = urlunsplit(('', parts.netloc, parts.path, parts.query, ''))
            
            if compareUrl not in self.seenURLs:
                self.seenURLs.add(compareUrl)
                return True
            return False
    
    def incrFinished(self):
        with self.finishedPagesLock:
            self.numFinishedPages += 1

    def ping(self):
        return True
    
    def pushGatheredURLandDesc(self, url, description):
        with self.finishedURLLock:
            if url not in self.gatheredURLs:
                self.gatheredURLs[url] = description

def getHTMLFromURL(url, timeout=10):
    headers = {"User-Agent": "WebScraperTest"}
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.text, resp.url

def extractPlainText(html):
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator=" ", strip=True)
    text = " ".join(text.split())
    return text

def cleanURL(url):
    parts = urlsplit(url)
    #https://docs.python.org/3/library/urllib.parse.html
    parts = (parts.scheme, parts.netloc, parts.path, parts.query, "")
    unsplit = urlunsplit(parts)
    if unsplit.endswith("/"):
        unsplit = unsplit[:-1]
    return unsplit

def extractLinks(html, baseUrl):
    soup = BeautifulSoup(html, "lxml")
    anchors = soup.find_all("a")
    results = []
    
    for a in anchors:
        href = a.get("href")
        
        if not href:
            continue
            
        href = href.strip()
        
        #if href.startswith("javascript:"):
        if href.startswith("javascript:") or href.startswith("#") or href.startswith("mailto:"):
            continue
            
        absoluteURL = urljoin(baseUrl, href)
        cleanedURL = cleanURL(absoluteURL)
        
        #anchorText = a.get_text()
        anchorText = a.get_text(strip=True)
        #use title if no anchor
        if not anchorText:
            anchorText = a.get("title", "").strip()
        #parse from URL if there's neither
        if not anchorText:
            anchorText = parseLabelFromURL(cleanedURL)
        results.append((cleanedURL, anchorText))
    return results

def scraperWorker(db,id):
    while not db.isShutdown():
        url = db.popUnsearched()
        if url is None:
            continue
        
        try:
            html, finalUrl = getHTMLFromURL(url)
            pageData = PageData(url=finalUrl, html=html)
            db.pushUnextractedPage(pageData)
            current_time = datetime.now().strftime("%H:%M:%S")
            print(f"[{current_time}] Scraper {id} scraped: {finalUrl}")
        except Exception as e:
            print(f"[{current_time}] Scraper {id} had an error scraping {url}: {e}")

def extractorWorker(db,id):
    while not db.isShutdown():
        pageData = db.popUnextractedPage()
        if pageData is None:
            continue
        
        try:
            uniqueCount = 0
            pageData.plaintext = extractPlainText(pageData.html)
            links = extractLinks(pageData.html, pageData.url)
            for linkUrl, anchorText in links:
                #remove files
                lower = linkUrl.lower()
                if any(lower.endswith(ext) for ext in ['.pdf', '.zip','.doc', '.docx', '.xls', '.xlsx', '.csv', '.ppt', '.pptx', '.mp3', '.mp4', '.jpg', '.jpeg', '.png', '.gif', '.svg', '.avi', '.mov', '.cgi', '.flv', '.ibooks']):
                    continue

                #skip if escaping domain
                if not db.isSameDomain(linkUrl):
                    continue

                if db.isBlacklisted(linkUrl):
                    continue
                
                if db.addToSeen(linkUrl):
                    db.pushUnsearched(linkUrl)
                    db.pushGatheredURLandDesc(linkUrl, anchorText)
                    uniqueCount += 1
            
            db.incrFinished()
            current_time = datetime.now().strftime("%H:%M:%S")
            print(f"[{current_time}] Extractor {id} processed {pageData.url}, extracting {len(links)} links / {uniqueCount} uniques")
        except Exception as e:
            print(f"[{current_time}] Extractor {id} had an error processing {pageData.url}: {e}")



def page_data_to_dict(obj):
    return {
        "__class__": "PageData",
        "url": obj.url,
        "html": obj.html
    }


def dict_to_page_data(classname, d):
    return PageData(d["url"],d["html"])


if __name__ == "__main__":

    config.SERIALIZER = "serpent"
    register_dict_to_class("PageData", dict_to_page_data)
    register_class_to_dict(PageData, page_data_to_dict)
    
    db = Database()
    db.setDomain(seed)
    print(f"Scraping domain: {db.domain}")
    print(f"Run time: {runTimeMinutes} minutes")
    current_time = datetime.now().strftime("%H:%M:%S")
    print(f"Current time: {current_time}\n")
    
    db.addToSeen(seed)
    db.pushUnsearched(seed)
    db.pushGatheredURLandDesc(seed, "DLSU Home Page")
    
    # Register database with Pyro
    daemon = Pyro5.api.Daemon(host=ip)
    ns = Pyro5.api.locate_ns(ip, 9090)
    uri = daemon.register(db)
    print(uri)
    ns.register("WebScraperDB", uri)
    print("Database server registered with Pyro")
    
    #start daemon request loop in its own thread
    daemonRequestThread = threading.Thread(target=daemon.requestLoop)
    daemonRequestThread.daemon = True
    daemonRequestThread.start()
    
    threads = []
    
    for i in range(nScrapers):
        t = threading.Thread(target=scraperWorker, args=(db,i))
        t.start()
        threads.append(t)
    
    for i in range(nExtractors):
        t = threading.Thread(target=extractorWorker, args=(db,i,))
        t.start()
        threads.append(t)

    #recording graph data
    db.seenURLsOverTime.append(0)  

    elapsed = 0
    while elapsed < runTimeMinutes * 60:
        time.sleep(recordTimeInterval)
        elapsed += recordTimeInterval
    
        seen = db.getSeenCount()
        unsearched = db.unsearchedURLs.qsize()
        unextracted = db.unextractedPages.qsize()
    
        db.seenURLsOverTime.append(seen)
        db.unsearchedSizeOverTime.append(unsearched)
        db.unextractedSizeOverTime.append(unextracted)
    
    db.shutdownFlag.set()
    
    for t in threads:
        t.join()

    print("\n")

    current_time = datetime.now().strftime("%H:%M:%S")
    print(f"End time: {current_time}")

    
    print(f"Number of pages scraped: {db.numFinishedPages}")
    print(f"Number of URLs found: {len(db.gatheredURLs)}")


    timestamp=datetime.now().strftime("%Y%m%d_%H%M%S")
    
    baseFilename=f"results/{timestamp}/"

    os.makedirs(baseFilename)
    
    csvFilename= f"{baseFilename}urls.csv"
    with open(csvFilename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['URL', 'Description'])
        
        for url, description in sorted(db.gatheredURLs.items()):
            writer.writerow([url, description])
    
    txtFilename = f"{baseFilename}results.txt"
    with open(txtFilename, 'w', encoding='utf-8') as txtfile:
        txtfile.write(f"Scraper Stats\n--------------------\n")
        txtfile.write(f"nScrapers: {nScrapers}\n")
        txtfile.write(f"nExtractors: {nExtractors}\n")
        txtfile.write(f"Number of pages scraped: {db.numFinishedPages}\n")
        txtfile.write(f"Number of URLs found: {len(db.gatheredURLs)}\n")
        txtfile.write(f"List of All Unique URLs Accessed:\n")
        for url in sorted(db.gatheredURLs.keys()):
            txtfile.write(f"{url}\n")
    
    timePoints = [i * recordTimeInterval for i in range(len(db.seenURLsOverTime))]
    with open(f"{baseFilename}stats.csv", "w", newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["time", "seen_url_count", "unsearched_count", "unextracted_count"])
    
        for t, count, us, ue in zip(timePoints, db.seenURLsOverTime, db.unsearchedSizeOverTime, db.unextractedSizeOverTime):
            writer.writerow([t, count, us, ue])
        
    print(f"Records saved under {baseFilename}")
    
    time.sleep(10)

    daemon.close()
