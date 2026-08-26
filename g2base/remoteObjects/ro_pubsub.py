#
# Choose internal version of pubsub
#

# re-exported on purpose: pub_sub.py imports PubSub from here, and
# switching backends means changing which of these lines is live
from .pubsubs.pubsub_redis import PubSub  # noqa: F401
#from .pubsubs.pubsub_zmq import PubSub
