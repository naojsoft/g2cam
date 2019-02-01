"""
This module abstracts the publish/subscribe transport used by
Subaru Remote Objects system.

USAGE:

Publisher
=========
import pubsub

ps = pubsub.PubSub('somehost')
ps.publish('foo', obj)

Subscriber
==========
import pubsub

def handler(ps, channel, data):
    print(channel, data)

ps = pubsub.PubSub('somehost')
ps.subscribe('foo')
# add a callback for messages to 'foo' channel
ps.add_callback('foo', handler)
# start a thread in a loop listening for messages
ps.start()

"""
