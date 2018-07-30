from flask import request, Response
from statbot import all_configurations
import psycopg2 as pg
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import re
import wikipedia as wiki
from textblob import TextBlob as tb
from nltk.corpus import stopwords
from threading import Thread
import requests
import json
from statbot import app
from statbot.loaders import forum_loader


stop_words = set(stopwords.words('english'))
emoji_pattern = re.compile(all_configurations.EMOTICONS, flags=re.UNICODE)

# import slackclient
# bot_slack_client = slackclient.SlackClient(bot_token)

# app = Flask(__name__)


@app.route('/', methods=['GET'])
def test():
    print(request.form)
    thr = Thread(target=forum_loader.run_forum_loader)
    thr.start()

    return "Slackbot is running.\n\n"


@app.route('/define', methods=['POST'])
def define_bot():
    print(request.form)
    if request.form.get('token') == all_configurations.DEFINE_TOKEN:
        # print(request.form)
        response_url = request.form.get('response_url')
        # search_db(request.form.get('text'), response_url)

        keywords = request.form.get('text')
        thr = Thread(target=search_db, args=[keywords, response_url])
        thr.start()

        return Response('*_' + keywords + ':_*\n')
        # return jsonify()

    return Response("You do not have access to this request."), 200


def search_db(keywords, response_url):

    # Remove stopwords, special characters, emojis, lowercase the keywords,
    # spelling correction, etc

    keywords = emoji_pattern.sub(r'', keywords)
    # print(1, keywords)
    keywords = keywords.strip().split()
    # print(2, keywords)
    keywords = [str(word.replace('[^\w\s]', '').lower()) for word in keywords if word not in stop_words]
    # print(3, keywords)
    keywords = (" ".join(keywords)).strip()
    # print(keywords)

    connection = pg.connect(
        host=all_configurations.HOST,
        user=all_configurations.USER,
        dbname=all_configurations.DATABASE,
        password=all_configurations.DB_PWD,
        port=all_configurations.PORT
    )
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cursor = connection.cursor()

    query = """SELECT {} FROM {} WHERE LOWER({}) LIKE '% {}%';""".format(
        'link',
        'forum_data',
        'topic',
        keywords
    )
    cursor.execute(query)
    forum_result = cursor.fetchall()

    # query = """SELECT {} FROM {} WHERE LOWER({}) = '{}';""".format(
    #     'content',
    #     'ipterms',
    #     'term',
    #     keywords
    # )
    # cursor.execute(query)
    # ipterm_result = cursor.fetchall()

    final_result = ""

    # if ipterm_result:
    #     final_result += ipterm_result[0][0]
    # else:
    #     print('{} not found in ipterms.'.format(keywords))

    try:
        final_result += wiki.summary(keywords)

    except wiki.DisambiguationError:

        text = "` Multiple results found. Choose a specific term from the ones given below. " \
               "(Please use the exact keywords to specify your query) :` \n\n"
        text += "\n".join(wiki.search(keywords))

        payload = {
            'text': text,
            'user': 'statbot'
        }
        requests.post(response_url, data=payload)
        return

    except wiki.PageError:
        split_words = keywords.split(" ")
        corrected_words = [tb(word.strip()).correct() for word in split_words]
        keywords = " ".join(corrected_words).strip()

        try:
            final_result += wiki.summary(keywords)
        except wiki.DisambiguationError:
            text = "` Multiple results found. Choose a specific term from the ones given below. " \
                   "(Please use the exact keywords to specify your query) :` \n\n"
            text += "\n".join(wiki.search(keywords))

            payload = {
                'text': text,
                'user': 'statbot'
            }
            requests.post(response_url, data=payload)
            return
        except wiki.PageError:
            pass
            payload = {
                'text': "Please ensure you've used the correct spelling and/or keywords.",
                'user': 'statbot'
            }
            requests.post(response_url, data=payload)
            return
        except Exception as e:
            print("Wiki exception occurred: ", e)
    except Exception as e:
        print("Wiki exception occurred: ", e)

    if forum_result:
        final_result += "\n\n\n ` Here are a few forum discussions related to the topic " \
                        "that may be useful: ` \n"

        num = 0
        for res in forum_result:
            num += 1
            if num > 10:
                final_result += "\n\n*Found {} related forum posts. To have the full list please use the _{}_ slash command.*".format(
                    len(forum_result),
                    all_configurations.FORUM_POST_SLASH_COMMAND
                )
                break
            final_result += " > " + res[0] + "\n"
    elif not final_result:
        final_result = "No results found. Please ensure you've used the correct spelling and/or keywords. " \
                       "\nOr try using more specific terms in your query."
    else:
        final_result += "\n\n\n ` NO RELATED FORUM POST FOUND. `"

    cursor.close()
    connection.close()

    # print(final_result)

    payload = {
        'text': final_result,
        'user': 'statbot'
    }

    requests.post(response_url, data=json.dumps(payload))


if __name__ == "__main__":

    app.run(debug=True)
