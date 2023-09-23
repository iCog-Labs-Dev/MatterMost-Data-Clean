from flask import Flask, render_template
from time import time, sleep
from sched import scheduler
import requests
import shelve

app = Flask(__name__)

mmUrl = "http://localhost:8065/api/v4"
loginData = {"login_id": "sysadmin", "password": "Sys@dmin-sample1"}

nextFetchScheduler = scheduler(time, sleep)

# --------------------- Routes ---------------------

@app.route("/")
def index():
    return render_template("index.html")

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
    # authHeader = "Bearer " + login().headers["token"]

    with shelve.open('pat') as db:
        if 'token' in db:
            token = db['token']
        else:
            token = ''

    authHeader = "Bearer " + token

    print('-'*20)
    print('-'*20)
    print('\n')
    
    channels = getChannels(authHeader) # get all channels

    with shelve.open('interval') as db:
        if 'interval' in db:
            interval = db['interval']
        else:
            interval = ''

    fetchIntervalInSeconds = interval * 60 # fetch interval in seconds  

    # Get the last fetch time from shelve file store
    with shelve.open('store') as db: # handles the closing of the shelve file automatically with context manager
        if 'lastFetchTime' in db:
            lastFetchTime = db['lastFetchTime']
        else:
            lastFetchTime = 0

    if lastFetchTime == 0: # This are no posts in the database
        print('get all posts for the first time')
        
        if not nextFetchScheduler.empty(): cancel() # cancel all previously scheduled events
        
        scheduleFirstEvent(fetchIntervalInSeconds, authHeader, channels) # schedule the first event
        
        nextFetchScheduler.run() # run the scheduled events
    
    else: 
        print('Not the first time getting posts')

        if nextFetchScheduler.empty(): 
            scheduleFirstEvent(fetchIntervalInSeconds, authHeader, channels)
            nextFetchScheduler.run()   
        
    return "OK"

# TODO: Add query param (userId) to get channels for a specific user id
@app.route("/user-channels")
def getUserChannels():
    # authHeader = "Bearer " + login().headers["token"]

    with shelve.open('pat') as db:
        if 'token' in db:
            token = db['token']
        else:
            token = ''

    authHeader = "Bearer " + token
    
    userId = login().json()['id']

    teams = fetchUserTeams(authHeader, userId)

    channels = []
    for team in teams:
        channel = fetchChannelsForUserTeam(authHeader, userId, team['id'])
        channels.extend(channel)

    channels = list({v['id']:v for v in channels}.values()) # make the channels list unique
    
    print('Total Channels: ', len(channels))

    return channels

@app.route('/set-interval/', defaults={'interval' : 5})
@app.route('/set-interval/<interval>')
def setInterval(interval):
    with shelve.open('interval') as db:
        db['interval'] = interval

    return 'The interval is: ' + str(interval)

@app.route('/set-personal-access-token/<token>')
def setPersonalAccessToken(token):
    with shelve.open('pat') as db:
        db['token'] = token

    return 'The token is: ' + str(token)

# --------------------- Helper Functions ---------------------

def login():
    res = requests.post(
        mmUrl + "/users/login",
        json=loginData,
        headers={"Content-type": "application/json; charset=UTF-8"},
    )

    return res

def getChannels(authHeader):
    channels = []
    queryChannels = fetchAllChannels(authHeader)

    # Filter out unnecessary channel properties 
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
    nextFetchScheduler.enter(
        0,
        1, # priority
        getPostsForAllChannels, # function to run when the event is triggered
        [fetchIntervalInSeconds, authHeader, nextFetchScheduler, channels] # arguments to pass to the function
    ) 

def getPostsForAllChannels(fetchIntervalInSeconds, authHeader, scheduler, channels):
    print('*'*50)
    print('\n')

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

    print('lastFetchTime from store: ', lastFetchTime)

    # calculate the time passed since lastFetchTIme
    timePassedInSeconds = (time() - lastFetchTime)
    print('Time passed since last fetch: ', timePassedInSeconds)

    postParams = {}

    if lastFetchTime != 0:
        postParams = { 'since': int(lastFetchTime * 1000) } # convert to milliseconds

    # Set the last fetch time to the current time for next api call
    with shelve.open('store') as db:
        db['lastFetchTime'] = time()

    posts = []

    print('postParams: ', postParams)
    for channel in channels:
        # reset page to 0 for each channel and set the number of posts per page to max (200)
        postParams.update({'per_page': 200, 'page': 0})

        # previousPostId is used to check if there are more pages of posts
        previousPostId = ' '

        # Loop through all pages of posts for the channel
        while previousPostId != '':
            postsRes = fetchPostsForChannel(authHeader, channel['id'], postParams)

            # Loop through each post in the response in order, filter out unnecessary post properties 
            for postId in postsRes['order']:
                post = {}
                tempPost = postsRes['posts'][postId]
                
                post['id'] = tempPost['id']
                post['root_id'] = tempPost['root_id']
                post['channel_id'] = tempPost['channel_id']
                post['create_at'] = tempPost['create_at']
                post['update_at'] = tempPost['update_at']
                post['message'] = tempPost['message']
                post['user_id'] = tempPost['user_id']

                posts.append(post) # add filtered post to the posts list
            
            # Update the page number and previousPostId for the next page of posts, if any
            postParams['page'] += 1
            previousPostId = postsRes['prev_post_id']
    
    print('Total Posts SUB: ', len(posts))

# --------------------- API Calls ---------------------

def fetchAllChannels(authHeader):
    res = requests.get(
        mmUrl + "/channels",
        headers={
            "Content-type": "application/json; charset=UTF-8",
            "Authorization": authHeader,
        },
    )
    
    if res.status_code != requests.codes.ok:
        print("Get all channels request failed with status code: ", res.status_code)
        return

    return res.json()

def fetchPostsForChannel(authHeader, channelId, postParams):
    res = requests.get(
        mmUrl + "/channels/" + channelId + "/posts",
        params=postParams,
        headers={
            "Content-type": "application/json; charset=UTF-8",
            "Authorization": authHeader,
        },
    )

    if res.status_code != requests.codes.ok:
        print("Get posts for a channel request failed with status code: ", res.status_code)
        return

    return res.json()

def fetchUserTeams(authHeader, userId):
    res = requests.get(
        mmUrl + "/users/" + userId + "/teams",
        headers={
            "Content-type": "application/json; charset=UTF-8",
            "Authorization": authHeader,
        },
    )

    if res.status_code != requests.codes.ok:
        print("Get User's teams request failed with status code: ", res.status_code)
        return

    return res.json()

def fetchChannelsForUserTeam(authHeader, userId, teamId):
    res = requests.get(
        mmUrl + "/users/" + userId + "/teams/" + teamId + "/channels",
        headers={
            "Content-type": "application/json; charset=UTF-8",
            "Authorization": authHeader,
        },
    )

    if res.status_code != requests.codes.ok:
        print("Get Channels for a User team request failed with status code: ", res.status_code)
        return

    return res.json()

# --------------------- Main ---------------------

if __name__ == "__main__":
    app.run(debug=True)
