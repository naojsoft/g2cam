#
# Choose internal version of pubsub
#

###############################################################
# NOTE: UNCOMMENT ONLY ONE!
###############################################################

from .pubsubs.pubsub_redis import PubSub
#from .pubsubs.pubsub_nats import PubSub
#from .pubsubs.pubsub_zmq import PubSub
