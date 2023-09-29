from flask import Flask, render_template, request
from time import time, sleep
from sched import scheduler
import requests
import shelve

app = Flask(__name__)

mmAPI = ""
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

@app.route('/set-interval', methods=['POST'])
def setInterval():
    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        json = request.get_json()

        if 'interval' not in json:
            return 'Interval not provided!'

        with shelve.open('interval') as db:
            db['interval'] = json['interval']

        return json
    else:
        return 'Content-Type not supported!'

@app.route('/set-mattermost-url', methods=['POST'])
def setMatterMostUrl():
    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        json = request.get_json()

        if 'mmUrl' not in json:
            return 'Mattermost URL not provided!'

        with shelve.open('mmUrl') as db:
            db['mmUrl'] = json['mmUrl']

        return json
    else:
        return 'Content-Type not supported!'

@app.route('/create-personal-access-token',)
def createPersonalAccessToken():
    loginRes = login()

    authHeader = "Bearer " + loginRes.headers["token"]
    userId = loginRes.json()['id']

    updateMatterMostUrl()

    res = requests.post(
        mmAPI + "/users/" + userId + "/tokens",
        headers={
            "Content-type": "application/json; charset=UTF-8",
            "Authorization": authHeader,
        },
        json={
            "description": "test token",
        },
    )

    print('Url: ', mmAPI + "/users/" + userId + "/tokens")
    
    if res.status_code != requests.codes.ok:
        print("Get all channels request failed with status code: ", res.status_code)
        return
    
    setAdminPersonalAccessToken(res.json()['token'])

    return res.json()

# --------------------- Helper Functions ---------------------

def updateMatterMostUrl():
    with shelve.open('mmUrl') as db:
        if 'mmUrl' in db:
            global mmAPI
            mmAPI = db['mmUrl'] + '/api/v4'
        else:
            mmAPI = 'http://localhost:8065/api/v4'

def setAdminPersonalAccessToken(token):
    with shelve.open('pat') as db:
        db['token'] = token

def login():
    updateMatterMostUrl()
    res = requests.post(
        mmAPI + "/users/login",
        json=loginData,
        headers={"Content-type": "application/json; charset=UTF-8"},
    )

    print('Url: ', mmAPI + "/users/login")

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
    updateMatterMostUrl()
    res = requests.get(
        mmAPI + "/channels",
        headers={
            "Content-type": "application/json; charset=UTF-8",
            "Authorization": authHeader,
        },
    )

    print('Url: ', mmAPI + "/channels")
    
    if res.status_code != requests.codes.ok:
        print("Get all channels request failed with status code: ", res.status_code)
        return

    return res.json()

def fetchPostsForChannel(authHeader, channelId, postParams):
    updateMatterMostUrl()
    res = requests.get(
        mmAPI + "/channels/" + channelId + "/posts",
        params=postParams,
        headers={
            "Content-type": "application/json; charset=UTF-8",
            "Authorization": authHeader,
        },
    )

    print('Url: ', mmAPI + "/channels/" + channelId + "/posts")

    if res.status_code != requests.codes.ok:
        print("Get posts for a channel request failed with status code: ", res.status_code)
        return

    return res.json()

def fetchUserTeams(authHeader, userId):
    updateMatterMostUrl()
    res = requests.get(
        mmAPI + "/users/" + userId + "/teams",
        headers={
            "Content-type": "application/json; charset=UTF-8",
            "Authorization": authHeader,
        },
    )

    print('Url: ', mmAPI + "/users/" + userId + "/teams")

    if res.status_code != requests.codes.ok:
        print("Get User's teams request failed with status code: ", res.status_code)
        return

    return res.json()

def fetchChannelsForUserTeam(authHeader, userId, teamId):
    updateMatterMostUrl()
    res = requests.get(
        mmAPI + "/users/" + userId + "/teams/" + teamId + "/channels",
        headers={
            "Content-type": "application/json; charset=UTF-8",
            "Authorization": authHeader,
        },
    )

    print('Url: ', mmAPI + "/users/" + userId + "/teams/" + teamId + "/channels")

    if res.status_code != requests.codes.ok:
        print("Get Channels for a User team request failed with status code: ", res.status_code)
        return

    return res.json()

# --------------------- Main ---------------------

if __name__ == "__main__":
    app.run(debug=True, port=5001)
