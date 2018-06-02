"""
iterates through graphtreon file to find URLs

downloads and extracts relevant <script> content
and saves to datadump/patreon/overviews/[first_letter_of_slug]/slug.html

skips existing files; should only have to run once
"""

from bs4 import BeautifulSoup
import csv
import requests
from pathlib import Path
from time import sleep

# http://skipperkongen.dk/2016/09/09/easy-parallel-http-requests-with-python-and-asyncio/
import asyncio
from concurrent.futures import ThreadPoolExecutor
BATCH_SIZE = 50
WORKER_COUNT = 10

GRAPHTREON_SRCPATH = Path('mydata', 'graphtreon', '2018-04.csv')
GRAPHTREON_HEADERS = [
    "Graphtreon","Name","Category","Patrons","Earnings","Range","Is Nsfw","Facebook Likes",
    "Twitter Followers","Youtube Subscribers","Youtube Videos","Youtube Views",
]

DATA_DIR = Path('datadump', 'patreon', 'overviews')
DATA_DIR.mkdir(parents=True, exist_ok=True)

ERRORS_PATH = Path('mydata', 'badpaths.csv')

METATAG = 'Object.assign(window.patreon.bootstrap, {'


def get_source_records(srcpath=GRAPHTREON_SRCPATH):
    records = []
    for d in csv.DictReader(GRAPHTREON_SRCPATH.read_text().splitlines()):
        # filter out invalid users
        if 'user?u=' not in d['Graphtreon']:
            d['slug'] = d['Graphtreon'].split('/')[-1]
            d['patreon_url'] = "https://www.patreon.com/{}/overview".format(d['slug'])
            records.append(d)
    return records


def filter_paths(allpaths):
    badurls = get_bad_urls()

    paths = []
    for p in allpaths:
        s, url, dest = p
        if not url in badurls and not dest.exists():
            paths.append(p)
    return paths


def gather_paths():
    """
    returns a list of tuples:
        [(slug, patreonurl, local_filename), (slug, patreon_url, local_filename)]
    """
    paths = []
    for row in get_source_records():
        slug = row['slug']
        url = row['patreon_url']
        dest_name = DATA_DIR.joinpath(slug[0].lower(), slug + '.html')
        paths.append((slug, url, dest_name))
    return paths


def __record_error(url, dest_name, error):
    with open(str(ERRORS_PATH), 'a') as w:
        slug = url.split('/')[-1]
        m = [url, str(dest_name), error]
        w.write(','.join(m))
        w.write('\n')


def get_bad_urls():
    return [e['patreon_url'] for e in csv.DictReader(ERRORS_PATH.read_text().splitlines())]

def extract(rawhtml):
    """
    too expensive to save entire HTML, so we save just the relevant script tags
    """
    soup = BeautifulSoup(rawhtml, 'lxml')
    scripttags = [str(s) for s in soup.select('script') if METATAG in s.text]
    return '\n'.join(scripttags)


def fetch_and_extract(url, dest_name):
    resp = requests.get(url, allow_redirects=False)
    if resp.status_code == 200:
        dest_name.parent.mkdir(exist_ok=True)
        with open(dest_name, 'w') as w:
            scripttext = extract(resp.text)
            w.write(scripttext)
            return {'success': True, 'content': scripttext, 'url': url, 'dest_name': dest_name}
    else:
        msg = str(resp.status_code)
        return {'success': False, 'content': msg, 'url': url, 'dest_name': dest_name}


def make_batches(mylist, num):
    """easy chunking
    https://chrisalbon.com/python/data_wrangling/break_list_into_chunks_of_equal_size/

    Generates list of lists, with each list having num elements
    """
    for i in range(0, len(mylist), num):
        # Create an index range for l of n items:
        yield mylist[i:i+num]



async def batch_fetch(batch):
    """
    From:
    http://skipperkongen.dk/2016/09/09/easy-parallel-http-requests-with-python-and-asyncio/

    batch is a list of tuples:
    [(slug, patreonurl, local_filename), (slug, patreon_url, local_filename)]

    """
    with ThreadPoolExecutor(max_workers=WORKER_COUNT) as executor:
        loop = asyncio.get_event_loop()
        futures = [
            loop.run_in_executor(
                executor,
                fetch_and_extract,
                url,
                dest_name,
            )
            for slug, url, dest_name in batch
        ]

        for d in await asyncio.gather(*futures):
            if d['success']:
                print("Downloaded:", d['url'])
                print("\tWrote:", len(d['content']), 'to:', d['dest_name'])
            else:
                __record_error(url=d['url'], dest_name=d['dest_name'], error=d['content'])
                print("Error with:", d['url'])
                print("\t", d['content'])

def main():
    all_paths = gather_paths()
    print("total paths:", len(all_paths))
    paths = filter_paths(all_paths)
    print("remaining paths:", len(paths))
    print("-------------------------\n\n")
    batches = list(make_batches(paths, BATCH_SIZE))



    for i, batch in enumerate(batches):
        print("\n\n")
        print("Batch {} of {}".format(i, len(batches)))
        print("---------------")
        loop = asyncio.get_event_loop()
        loop.run_until_complete(batch_fetch(batch))
        sleep(1)



if __name__ == '__main__':
    main()


