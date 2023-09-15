from flask import Flask, render_template
from time import time, sleep
from sched import scheduler
import requests
import shelve

app = Flask(__name__)

mmUrl = "http://localhost:8065/api/v4"
loginData = {"login_id": "sysadmin", "password": "Sys@dmin-sample1"}

nextFetchScheduler = scheduler(time, sleep)

@app.route("/")
def index():
    return render_template("index.html")

# authenticate a user
def login():
    res = requests.post(
        mmUrl + "/users/login",
        json=loginData,
        headers={"Content-type": "application/json; charset=UTF-8"},
    )

    return res

def getChannels(authHeader):
    res = requests.get(
        mmUrl + "/channels",
        headers={
            "Content-type": "application/json; charset=UTF-8",
            "Authorization": authHeader,
        },
    )

    # Guard against bad requests
    if res.status_code != requests.codes.ok:
        print("Request failed with status code: ", res.status_code)
        return

    queryChannels = res.json()
    channels = []

    # Filter out the channel properties we don't want
    for channel in queryChannels:
        tempChannel = {}

        tempChannel["id"] = channel["id"]
        tempChannel["name"] = channel["name"]
        tempChannel["display_name"] = channel["display_name"]
        tempChannel["type"] = channel["type"]
        tempChannel["create_at"] = channel["create_at"]
        tempChannel["creator_id"] = channel["creator_id"]
        tempChannel["last_post_at"] = channel["last_post_at"]
        tempChannel["total_msg_count"] = channel["total_msg_count"]

        channels.append(tempChannel)
    
    totalPosts = 0
    for channel in channels:
        totalPosts += channel['total_msg_count']

    print('Total Channel: ', len(queryChannels))
    print('Total Posts: ', totalPosts)

    return channels

def scheduleFirstEvent(fetchIntervalInSeconds, authHeader, channels):
    print('scheduleFirstEvent')
    # Add an event to the scheduler
    nextFetchScheduler.enter(
        0,
        1, # priority
        getPostsForAllChannels, # function to run when the event is triggered
        [fetchIntervalInSeconds, authHeader, nextFetchScheduler, channels] # arguments to pass to the function
    ) 

def getPostsForAllChannels(fetchIntervalInSeconds, authHeader, scheduler, channels):
    print('\n')
    print('*'*50)
    print('\n')
    print('getPostsForAllChannels')

    scheduler.enter(
        fetchIntervalInSeconds, 
        1, 
        getPostsForAllChannels, 
        [fetchIntervalInSeconds, authHeader, scheduler, channels]
    )

    # Get the last fetch time from shelve file store
    with shelve.open('store') as db: # handles the closing of the shelve file automatically with context manager
        if 'lastFetchTime' in db:
            lastFetchTime = db['lastFetchTime']
        else:
            lastFetchTime = 0

    print('current time', time())
    print('lastFetchTime from store: ', lastFetchTime)

    # calculate the time passed since lastFetchTIme
    timePassedInSeconds = (time() - lastFetchTime)
    print('Time passed since last fetch SUB: ', timePassedInSeconds)

    postParams = {}

    # if timePassedInSeconds >= fetchIntervalInSeconds and lastFetchTime != 0:
    if lastFetchTime != 0:
        postParams = { 'since': int(lastFetchTime * 1000) } # convert to milliseconds
        print('get posts since last fetch time')

    # Set the last fetch time to the current time for next api call
    with shelve.open('store') as db:
        db['lastFetchTime'] = time()

    posts = []

    print('is channels empty: ', channels == [])
    print('postParams: ', postParams)
    for channel in channels:
        # 200 is the max number of posts per page
        # reset page to 0 for each channel
        postParams.update({'per_page': 200, 'page': 0})

        # previousPostId is used to check if there are more pages of posts
        previousPostId = ' '

        # Loop through all pages of posts for the channel
        while previousPostId != '':
            # Get the server response for each page of posts
            postsRes = requests.get(
                mmUrl + "/channels/" + channel["id"] + "/posts",
                params=postParams,
                headers={
                    "Content-type": "application/json; charset=UTF-8",
                    "Authorization": authHeader,
                },
            )

            # Guard against bad requests
            if postsRes.status_code != requests.codes.ok:
                print("Request failed with status code: ", postsRes.status_code)
                return

            # Convert the response to JSON
            postsRes = postsRes.json()

            # Loop through each post in the response in order, filter out the properties we don't want
            for postId in postsRes['order']:
                post = {}
                tempPost = postsRes['posts'][postId]
                
                # Filter out the post properties we don't want
                post['id'] = tempPost['id']
                post['root_id'] = tempPost['root_id']
                post['channel_id'] = tempPost['channel_id']
                post['create_at'] = tempPost['create_at']
                post['update_at'] = tempPost['update_at']
                post['message'] = tempPost['message']
                post['user_id'] = tempPost['user_id']

                # Add the filtered out post to the posts list
                posts.append(post)
            
            # Update the page number and previousPostId for the next page of posts
            postParams['page'] += 1
            previousPostId = postsRes['prev_post_id']
    
    print('Total Posts SUB: ', len(posts))


@app.route("/cancel")
def cancel():
    print('cancel')
    
    if not nextFetchScheduler.empty():
        for event in nextFetchScheduler.queue:
            # print('event: ', event)
            nextFetchScheduler.cancel(event)

    print('isEmpty: ', nextFetchScheduler.empty())
    return 'cancel'

@app.route("/posts")
def getAllPosts():
    authHeader = "Bearer " + login().headers["token"]
    print('\n')
    print('-'*20)
    print('-'*20)
    print('\n')
    
    channels = getChannels(authHeader) # get all channels
    fetchIntervalInSeconds = 3 * 60 # fetch interval in seconds  

    # Get the last fetch time from shelve file store
    with shelve.open('store') as db: # handles the closing of the shelve file automatically with context manager
        if 'lastFetchTime' in db:
            lastFetchTime = db['lastFetchTime']
        else:
            lastFetchTime = 0
    
    # calculate the time passed since lastFetchTIme
    timePassedInSeconds = (time() - lastFetchTime)
    print('Time passed since last fetch MAIN: ', timePassedInSeconds)

    if lastFetchTime == 0: # This are no posts in the database
        print('get all posts for the first time')
        
        cancel() # cancel all previously scheduled events
        
        scheduleFirstEvent(fetchIntervalInSeconds, authHeader, channels) # schedule the first event
        
        nextFetchScheduler.run() # run the scheduled events
    
    else: 
        print('Not the first time getting posts')

        if nextFetchScheduler.empty(): 
            scheduleFirstEvent(fetchIntervalInSeconds, authHeader, channels)
            nextFetchScheduler.run()   

    if timePassedInSeconds < fetchIntervalInSeconds:
        print("It's not time to fetch posts yet")
        
    return "OK"


if __name__ == "__main__":
    app.run(debug=True)