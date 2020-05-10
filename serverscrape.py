#!/usr/bin/env python3
#
#CHANGE THESE VARIABLES TO CONFIGURE
#
influxHost = 'localhost'
influxPort = 8086
influxUser = 'username'
influxPassword = 'password'
elasticHost = 'localhost'

discordToken = 'your token here'
databasePrefix = 'serverscrape' # Script will create or use databases named databasePrefix_guildID

#Done with config variables

import discord
from elasticsearch import Elasticsearch, helpers
import sys
from influxdb import InfluxDBClient
import datetime
import json
influx = InfluxDBClient(influxHost, influxPort, influxUser, influxPassword)
elastic =  Elasticsearch([
    {'host':elasticHost}
])
client = discord.Client()
guildToScrape = int(sys.argv[1])

def buildElasticDocFromMessage(message):
    indexName = '{0}_{1}'.format(databasePrefix, message.guild.id)
    attachments = []
    for attachment in message.attachments:
        attachments.append(attachment.url)
    measurement = 'chatMessage'
    authorID = message.author.id
    authorName = message.author.name
    serverID = message.guild.id
    serverName = message.guild.name
    channelID = message.channel.id
    channelName = message.channel.name
    messageID = message.id
    messageTextRaw = message.content
    messageText = message.clean_content
    messageLength = len(message.clean_content)
    timestamp = int((message.created_at - datetime.datetime(1970, 1, 1)).total_seconds() * 1000)
    createObj  = {
        "create": {
            "_index":indexName,
            "_id":message.id
        }
    }
    docObject = {
        "type":'default',
        "timestamp":timestamp,
        "messageSent":1,
        "authorID":authorID,
        "authorName":authorName,
        "messageID":messageID,
        "messageLength":messageLength,
        "messageTextRaw":messageTextRaw,
        "messageText":messageText,
        "channelID":channelID,
        "channelName":channelName,
        "serverID":serverID,
        "serverName":serverName,
        "attachments":', '.join(attachments)
    }
    final = '{0}\n{1}'.format(json.dumps(createObj), json.dumps(docObject))
    return final

def buildInfluxPointFromMessage(message):
    # Grab a bunch of variables to assemble our influx-compatible data point
    # 
    attachments = []
    for attachment in message.attachments:
        attachments.append(attachment.url)
    measurement = 'chatMessage'
    authorID = message.author.id
    authorName = message.author.name
    serverID = message.guild.id
    serverName = message.guild.name
    channelID = message.channel.id
    channelName = message.channel.name
    messageID = message.id
    messageTextRaw = message.content
    messageText = message.clean_content
    messageLength = len(message.clean_content)
    timestamp = int((message.created_at - datetime.datetime(1970, 1, 1)).total_seconds() * 1000)
    object = {
        "measurement": measurement,
        "tags": {
            "type":"default",
            "authorID":authorID,
            "authorName":authorName,
            "serverID":serverID,
            "serverName":serverName,
            "channelID":channelID,
            "channelName":channelName,
        },
        "time":timestamp,
        "fields": {
            "messageSent":1,
            "authorID":authorID,
            "authorName":authorName,
            "messageID":messageID,
            "messageLength":messageLength,
            "messageTextRaw":messageTextRaw,
            "messageText":messageText,
            "channelID":channelID,
            "channelName":channelName,
            "serverID":serverID,
            "serverName":serverName,
            "attachments":', '.join(attachments)
        }
    }
    return object


def checkListOfDict(list, key, value):
    for dict in list:
        if dict[key] == value:
            return True
    return False


async def influxDBInit(guildID):
    dbName = '{0}_{1}'.format(databasePrefix,guildID)
    print(influx.get_list_database())
    if not checkListOfDict(influx.get_list_database(), 'name', dbName):
        influx.create_database(dbName)
        influx.switch_database(dbName)
        print('Created influxDB database {0}'.format(dbName))
    else:
        influx.switch_database(dbName)
async def elasticInit(guildID):
    indexName = '{0}_{1}'.format(databasePrefix, guildID)
    elastic.indices.create(index=indexName, ignore=400)

async def scrapeChannel(channel):
    dbName = '{0}_{1}'.format(databasePrefix, channel.guild.id)
    print('Scraping channel {0}'.format(channel.name))
    lastMessage = channel.last_message
    iterator = 1000
    lastGroup = False
    messageCount = 0
    batches = 0
    influxBatchBuffer = []
    elasticBatchBuffer = ''
    while lastGroup == False:
        # Grab 1000 messages, before the last message
        # The last message being either the most recent message
        # or the last message in the batch we last grabbed.
        #
        # If there's less than 1000 messages in a batch,
        # assume we're at the end of the channel. Set the lastGroup
        # variable so that our loop terminates.
        #
        messageBatch = await channel.history(limit=1000,before=lastMessage).flatten()
        batches = batches + 1
        print("{0} Messages scraped.".format(messageCount))
        sys.stdout.write("\033[F")
        iterator = 1000
        if len(messageBatch) < 1000:
            lastGroup = True
            iterator = len(messageBatch)
        for message in messageBatch:
            influxLine = buildInfluxPointFromMessage(message)
            elasticDoc = buildElasticDocFromMessage(message)
            influxBatchBuffer.append(influxLine)
            elasticBatchBuffer = '{0}\n{1}'.format(elasticBatchBuffer, elasticDoc)
            messageCount = messageCount + 1
            iterator = iterator - 1
            if iterator == 0:
                # This is the last message in a batch, so we set the new
                # lastMessage as the baseline for our next batch grab, and write
                # our 1000 data points to influx in a large batch job.
                #
                lastMessage = message
                elastic.bulk(elasticBatchBuffer)
                influx.write_points(influxBatchBuffer,database=dbName,protocol=u'json')
    sys.stdout.write("\n")
    print('{0} Total messages scraped.'.format(messageCount))


def getReadableChannels(guild):
    member = guild.get_member(client.user.id)
    channelsToScrape = []
    for channel in guild.channels:
        # Check if channel is text type, and we have message history rights
        # 
        if str(channel.type) in 'text' and channel.permissions_for(member).read_message_history:
            channelsToScrape.append(channel)
    return channelsToScrape


@client.event
async def on_ready():
    print('Connected as {0.user}'.format(client))
    # Are we a member of the guild provided from the command line?
    #
    if client.get_guild(guildToScrape):
        print('Found the provided guild.')
        workingGuild = client.get_guild(guildToScrape)
        await influxDBInit(workingGuild.id)
        # Get only text channels, discarding topics, voice, whatever
        # 
        readableChannels = getReadableChannels(workingGuild)
        for channel in readableChannels:
            # scrape and store messages in channels
            # 
            await scrapeChannel(channel)
    else:
        print('{0.user} does not appear to be in the provided guild'.format(client) )


client.run(discordToken)
