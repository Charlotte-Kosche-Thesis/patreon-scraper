"""
Reads all files in datadump/patreon/overviews/**/*.html

Assumes overviews/somefilename.html only contains <script> tags with
 relevant data

Creates a CSV in results/with_keywords.csv
"""
from pathlib import Path
from fetch import GRAPHTREON_SRCPATH, get_source_records, GRAPHTREON_HEADERS
import csv

DATASTASH = Path('datadump', 'patreon', 'overviews')
SRC_FILES = list(DATASTASH.glob('**/*.html'))
DEST_PATH = Path('mydata', 'with_keywords.csv')
DEST_PATH.parent.mkdir(parents=True, exist_ok=True)

KEYWORDS_PATH = Path('keywords.txt')
KEYWORDS = KEYWORDS_PATH.read_text().splitlines()

RESULTS_HEADERS = ["slug", "patreon_url", "keywords_count", "keywords_found"] + GRAPHTREON_HEADERS

SRC_DATAPATH = GRAPHTREON_SRCPATH
DEST_DATAPATH = Path('mydata', 'detected-{}.csv'.format(SRC_DATAPATH.stem))
FIL_DATAPATH =  Path('mydata', 'filtered-detected-{}.csv'.format(SRC_DATAPATH.stem))

# def augment(records, src=SRC_DATAPATH, dest=DEST_DATAPATH):
#     """
#     for each row in records, finds corresponding row in
#         the original graphtreon file (GRAPHTREON_SRCPATH)
#         and adds 3 columns:
#             'slug', 'keywords_count', 'keywords_found'
#     """

#     return dest




def detect_keywords(rawhtml):
    rawtext = rawhtml.lower()
    # note that for creators with no currently running projects
    # len(scripts) should be 0, e.g.
    # datadump/patreon/overviews/t/thepenumbrapodcast.html
    foundwords = [w for w in KEYWORDS if w in rawtext]
    return foundwords


def filter(srcfiles):
    results = []
    for i, fname in enumerate(srcfiles):
        rawhtml = fname.read_text()
        try:
            foundwords = detect_keywords(rawhtml)
        except IndexError:
            # this is just a debug note, program goes on as usual
            print("ERROR: No <script> in:", fname)
        else:
            d = {}
            d['slug'] = fname.stem
            d['keywords_count'] = len(foundwords)
            d['keywords_found'] = ';'.join(foundwords)
            d['Graphtreon'] = 'https://graphtreon.com/creator/{}'.format(d['slug'])
            d['patreon_url'] = 'https://www.patreon.com/{}/overview'.format(d['slug'])

            if foundwords:
                results.append(d)
                print("{}/{}".format(i, len(SRC_FILES)),'\t', fname, "\n\tfound:", foundwords)
    ###
    return results

def main():
    srcfiles = SRC_FILES
    print(len(srcfiles), 'scraped HTML files found...\n')

    frows = filter(srcfiles)
    print('--------------------')
    print(len(frows), 'files with keywords found...\n')

    print("Writing to:", DEST_DATAPATH)

    # get the original records
    source_records = get_source_records()




    dw = open(DEST_DATAPATH, 'w')
    fw = open(FIL_DATAPATH, 'w')

    cdw = csv.DictWriter(dw, fieldnames=RESULTS_HEADERS)
    cdw.writeheader()
    cfw = csv.DictWriter(fw, fieldnames=RESULTS_HEADERS)
    cfw.writeheader()

    for row in source_records:
        d = next((x for x in frows if x['slug'] == row['slug']), False)
        if d:
            row['keywords_count'] = d['keywords_count']
            row['keywords_found'] = d['keywords_found']
            cfw.writerow(row)
        else:
            row['keywords_count'] = 0
            row['keywords_found'] = ""
        cdw.writerow(row)

    print("Wrote detected-keywords columns to:", DEST_DATAPATH)

    dw.close()
    fw.close()

if __name__ == '__main__':
    main()
