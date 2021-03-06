from requests import session
import requests
import json
from statbot import all_configurations
from bs4 import BeautifulSoup
import re
import psycopg2 as pg
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from datetime import datetime


def run_forum_loader():

    emoji_pattern = re.compile(all_configurations.EMOTICONS, flags=re.UNICODE)

    payload = {
        'email': all_configurations.EMAIL,
        'password': all_configurations.PASSWORD
    }

    connection = pg.connect(
        host=all_configurations.HOST,
        user=all_configurations.USER,
        dbname=all_configurations.DATABASE,
        password=all_configurations.DB_PWD,
        port=all_configurations.PORT
    )
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cursor = connection.cursor()

    # try:
    #     cursor.execute("""Truncate "forum_data";""")
    # except Exception as e:
    #     pass

    cursor.execute('SELECT post_id FROM {};'.format(
        all_configurations.FORUM_TABLE
    ))
    pids = [int(pid[0]) for pid in cursor.fetchall()]

    result = "Before running loader : " + str(len(pids))
    print(result)

    new_ids = []

    import os
    if not os.path.exists("logs/ud/"):
        os.makedirs("logs/ud/")

    f = open("logs/ud/udfailure{}.txt".format(datetime.now().strftime('%Y%m%d%H%M%S')), 'w')

    with session() as c:

        url = all_configurations.UDACITY_SIGNIN_URL
        c.post(url, data=json.dumps(payload))

        cnt = 0
        count = 0

        for j in all_configurations.UDACITY_FORUM_TABS:
            for i in range(500):

                print("\n PAGE - {} \t ({})\n".format(i + 1, j))

                response = c.get(all_configurations.UD_FORUM_URL.format(j, i))

                s = BeautifulSoup(response.text, 'html.parser')

                divs = s.find_all('div', {"itemprop": 'itemListElement'})
                if not divs:
                    break

                for div in divs:
                    link = div.find('a').get('href')
                    topic = emoji_pattern.sub(r'', div.find('span').text).strip()
                    topic = " ".join(re.sub(all_configurations.SPECIAL_CHARACTERS, ' ', topic).split())
                    post_id = link.split('/')[-1]

                    if (int(post_id) in pids) or (int(post_id) in new_ids):
                        # print("Repeated {} from {}.".format(link, j))
                        continue

                    query = """INSERT INTO {}(post_id, topic, link) VALUES ({}, '{}', '{}');""".format(
                        all_configurations.FORUM_TABLE,
                        post_id,
                        topic.strip(),
                        link,
                    )
                    # print(query)

                    try:
                        cursor.execute(query)
                        cnt += 1
                        new_ids.append(int(post_id))
                        # print("Added {} posts to db. Discarded {} till now.".format(cnt, count))
                    except Exception as e:
                        count += 1
                        # print("\nUnable to add post {} to db.\n".format(link))
                        f.write(link)
                    finally:
                        connection.commit()

    cursor.execute('SELECT * FROM {};'.format(all_configurations.FORUM_TABLE))
    result += "\nAfter running loader : " + str(cursor.rowcount)

    connection.commit()
    f.close()
    cursor.close()
    connection.close()

    print(result)


if __name__ == '__main__':
    run_forum_loader()

