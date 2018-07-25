from flask import Flask, request, Response
import config
import MySQLdb
import re
import wikipedia as wiki
from textblob import TextBlob as tb
from nltk.corpus import stopwords


stop_words = set(stopwords.words('english'))
emoji_pattern = re.compile(config.EMOTICONS, flags=re.UNICODE)

# import slackclient
# bot_slack_client = slackclient.SlackClient(bot_token)

app = Flask(__name__)


@app.route('/', methods=['GET'])
def test():
    return "Slackbot is running."


@app.route('/define', methods=['POST'])
def define_bot():
    if request.form.get('token') == config.DEFINE_TOKEN:
        return Response(search_db(request.form.get('text')))

    return Response("You do not have access to this request."), 200


def search_db(keywords):

    # Remove stopwords, special characters, emojis, lowercase the keywords,
    # spelling correction, etc

    keywords = emoji_pattern.sub(r'', keywords)
    print(1, keywords)
    keywords = keywords.strip().split()
    print(2, keywords)
    keywords = [str(tb(word.replace('[^\w\s]', '').lower()).correct()) for word in keywords if word not in stop_words]
    print(3, keywords)
    keywords = (" ".join(keywords)).strip()
    print(keywords)

    db = MySQLdb.connect(
        host=config.HOST,
        user=config.USER,
        db=config.DATABASE
    )

    cursor = db.cursor()

    query = """SELECT {} FROM {} WHERE LOWER({}) LIKE "%{}%";""".format(
        'link',
        'forum_data',
        'topic',
        keywords
    )
    cursor.execute(query)
    forum_result = cursor.fetchall()

    query = """SELECT {} FROM {} WHERE LOWER({}) = "{}";""".format(
        'content',
        'ipterms',
        'term',
        keywords
    )
    cursor.execute(query)
    ipterm_result = cursor.fetchall()

    final_result = ""

    if ipterm_result:
        final_result += ipterm_result[0][0]
    else:
        try:
            final_result += wiki.summary(keywords)
        except wiki.DisambiguationError:
            final_result += wiki.summary(keywords + ' (statistics)')

    if forum_result:
        final_result += "\n\nHere are a few forum discussions related to the topic " \
                        "that may be useful: \n"
        for res in forum_result:
            final_result += res[0] + "\n"

    cursor.close()

    print(final_result)
    return final_result


if __name__ == "__main__":

    app.run(debug=True)
