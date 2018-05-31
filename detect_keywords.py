"""
Reads all files in datadump/patreon/overviews

Assumes overviews/somefilename.html only contains <script> tags with
 relevant data

Creates a CSV in results/with_foundwords.csv
"""

from pathlib import Path
import csv
DATASTASH = Path('datadump', 'patreon', 'overviews')
SRC_FILES = list(DATASTASH.glob('*.html'))
DEST_PATH = Path('results', 'with_keywords.csv')
DEST_PATH.parent.mkdir(parents=True, exist_ok=True)

HEADERS = ['slug', 'keywords_count', 'keywords', 'patreon_url', 'Graphtreon']

KEYWORDS = ['journalism',
                'journalist',
                'reporter',
                'reporting',
                'investigative',
                'investigation',
                ]


def detect_keywords(rawhtml):
    rawtext = rawhtml.lower()
    # note that for creators with no currently running projects
    # len(scripts) should be 0, e.g.
    # datadump/patreon/overviews/thepenumbrapodcast.html
    foundwords = [w for w in KEYWORDS if w in rawtext]
    return foundwords

def main():
    print("Writing to:", DEST_PATH)
    f = open(DEST_PATH, 'w')
    cv = csv.DictWriter(f, fieldnames=HEADERS)
    cv.writeheader()

    for i, fname in enumerate(SRC_FILES):
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
            d['keywords'] = ';'.join(foundwords)
            d['Graphtreon'] = 'https://graphtreon.com/creator/{}'.format(d['slug'])
            d['patreon_url'] = 'https://www.patreon.com/{}/overview'.format(d['slug'])

            cv.writerow(d)
            if foundwords:
                print("{}/{}".format(i, len(SRC_FILES)),'\t', fname, "\tfound:", foundwords)
    f.close()

if __name__ == '__main__':
    main()
