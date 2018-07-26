import psycopg2 as pg
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from bs4 import BeautifulSoup
import requests
import json
import config
from datetime import datetime


letter_terms = {}
prev = 0

for letter in range(97, 123):

    letter = chr(letter)
    try:
        f = open("ipterms/{}.txt".format(letter), 'r')
        letter_terms = dict(json.loads(f.read()))
        f.close()
        print("Found '{}' terms. We have total {} terms now.".format(letter, len(letter_terms)))
        prev = len(letter_terms)
        continue
    except FileNotFoundError:
        print("'{}' terms not found. Scraping them.".format(letter))
        pass

    page = requests.get(config.IPTERM_URL.format(letter, 0))
    page = page.text
    soup = BeautifulSoup(page, 'html.parser')

    try:
        total_terms = soup.find('li', {"class": "terms"}).text.strip().split()[-2].replace(',', '')
    except Exception as e:
        print("\nSetting total_terms = 10 for {}\n".format(letter))
        total_terms = 10

    limit = int(total_terms) // 100 + 1
    # print(limit)

    for i in range(limit):
        page = requests.get(config.IPTERM_URL.format(letter, i))
        page = page.text
        soup = BeautifulSoup(page, 'html.parser')
        ol = soup.find('ol')
        for li in ol.find_all('li'):
            letter_terms[li.text.strip()] = li.a.get('href')

    print(letter, " - ", len(letter_terms) - prev, " DONE!")
    prev = len(letter_terms)

    f = open('{}.txt'.format(letter), 'w')
    f.write(json.dumps(letter_terms))
    print("\nAdded {} terms till '{}' in local file.\n".format(len(letter_terms), letter))
    f.close()


print("\nTotal number of terms on IP = {}\n".format(len(letter_terms)))
# print(letter_terms)

connection = pg.connect(
    host=config.HOST,
    user=config.USER,
    dbname=config.DATABASE
)
connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cursor = connection.cursor()

cursor.execute('SELECT link FROM {};'.format(
    config.IPTERMS_TABLE
))
tlinks = [tlink[0] for tlink in cursor.fetchall()]

# cursor.execute("INSERT INTO ipterms VALUES (4, 'aaa', 'aaa.aaa.aaa', 'aaa aaa aaa aaa');")
# cursor.execute('SELECT * FROM ipterms;')
# print(cursor.fetchall())

count = 0
c = 0
found = 0

f = open("logs/ipfailure{}.txt".format(datetime.now().strftime('%Y%m%d%H%M%S')), 'w')
f2 = open('logs/ipfound{}.txt'.format(datetime.now().strftime('%Y%m%d%H%M%S')), 'w')

for key, value in letter_terms.items():

    url = config.IP_PREURL + value

    if url in tlinks:
        found += 1
        f2.write(url+"\n")
        continue

    page = requests.get(url)
    page = page.text
    soup = BeautifulSoup(page, 'html.parser')

    paragraphs = [p.text.strip() for p in soup.find_all('p') if p.find('div') is None]
    # print(paragraphs[1:])
    content = ' '.join(paragraphs[1:])

    query = """INSERT INTO {}(link, content, term) VALUES ('{}', '{}', '{}');""".format(
        config.IPTERMS_TABLE,
        url,
        content.replace('\'', '\"'),
        key.replace('\'', '\"'),
    )
    # print(query)

    try:
        cursor.execute(query)
        c += 1
        print("Added {} terms to db. Discarded = {}".format(c, count))
    except Exception as e:
        count += 1
        print("\nUnable to add {} to db. Total terms discarded till now = {}\n".format(key, count))
        f.write(url+"\n")
    finally:
        connection.commit()

print("{} terms found.".format(found))

cursor.execute('SELECT * FROM {};'.format(config.IPTERMS_TABLE))
print(cursor.rowcount)

connection.commit()
f.close()
f2.close()
cursor.close()
connection.close()
