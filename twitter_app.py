import argparse
import socket
import time
import requests
import traceback
import json
from datetime import date
from datetime import timedelta

def authentication() -> str:
    
    # Bearer token from twitter 
    return "AAAAAAAAAAAAAAAAAAAAAH0jkAEAAAAAQs%2FTFeasYfHgnCnMp6cNQtZV3t8%3DOAOAIEB316gEpZHd3gU1rMF2DoMNLEjX7gidP8ejougo3prhcs"

def twitter(keyword, end_date, next_token=None, max_results=10):
    
    search_url = "https://api.twitter.com/2/tweets/search/recent"

    query_params = {'query': keyword,
                    'end_time': end_date,
                    'max_results': max_results,
                    'tweet.fields': 'id,text,author_id,geo,conversation_id,created_at,lang,entities',
                    'next_token': next_token
                    }
    return (search_url, query_params)

def get_response(url, headers, params):
    
    response = requests.get(url, headers=headers, params=params)
    print(f"Response Status Code: {str(response.status_code)}")
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def get_data(next_token=None, query='corona', max_results=20):
    
    bearer_token = authentication()
    headers = {"Authorization": f"Bearer {bearer_token}"}
    keyword = f"{query} lang:en has:hashtags"

    end_date = str(date.today() - timedelta(days=6))
    end_time = f"{end_date}T00:00:00.000Z"

    url: tuple = twitter(keyword, end_time, next_token=next_token, max_results=20)
    json_response = get_response(url=url[0], headers=headers, params=url[1])
    with open('test.txt', 'w+') as teeee:
        json.dump(json_response, teeee, indent=2)

    return json_response

def get_hashtag(tag_info: dict):

    hashtag_1 = str(tag_info['tag']).strip()
    hashtag_2 = str('#' + hashtag_1 + '\n')
    print(f"Hashtag: {hashtag_2.strip()}")
    return hashtag_2

def spark_twitter(http_resp, tcp_connection):

    data: list = http_resp["data"]

    for tweet in data:

        try:
            hashtag_list = tweet['entities']['hashtags']
            for tag_info in hashtag_list:
                hashtag_2 = get_hashtag(tag_info)
                tcp_connection.send(hashtag_2.encode("utf-8"))
        except KeyError:
            print("No hashtag found")
            continue
        except BrokenPipeError:
            exit("Broken pipe")
        except KeyboardInterrupt:
            exit("Keyboard Interrupt")
        except Exception as e:
            traceback.print_exc()

def input_term():

    parser = argparse.ArgumentParser(description='Spark Tweet analyzer')
    parser.add_argument('-p', '--pages', type=int, help="No of pages to query", required=True)
    parser.add_argument('-k', '--keywords', type=str, help="List of keywords to query", required=True)
    parser.add_argument('-m', '--max_results', type=int, help="max results", required=False)
    parser.add_argument('-s', '--sleep_timer', type=int, help="sleep timer", required=False)

    args = parser.parse_args()
    return args.pages, args.keywords, args.max_results, args.sleep_timer

if __name__ == '__main__':

    no_of_pages, queries, max_results, sleep_timer = input_term()
    if max_results is None:
        max_results = 20
    if sleep_timer is None:
        sleep_timer = 5

    queries = str(queries).split(" ")

    TCP_IP = "127.0.0.1"
    TCP_PORT = 9009

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((TCP_IP, TCP_PORT))
    sock.listen(1)
    print("Waiting for the TCP connection...")

    conn, addr = sock.accept()
    print("Successfully connected")
    next_token = None

    for query in queries:

        for _ in range(no_of_pages):

            try:
                print(f"\n\n\t\tProcessing Page {_} for keyword {query}\n\n")
                resp = get_data(next_token=next_token, query=query, max_results=max_results)
                next_token = resp['meta']['next_token']
                spark_twitter(http_resp=resp, tcp_connection=conn)
                time.sleep(sleep_timer)
            except KeyboardInterrupt:
                exit("Keyboard Interrupt")
