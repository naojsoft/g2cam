#
# ro_packer.py -- remote objects packing protocol
#
# This is open-source software licensed under a BSD license.
# Please see the file LICENSE.txt for details.
#
"""
The remote objects packing protocol is designed to be flexible enough
to handle a variety of packing needs.

Packer
------
pack(envelope, pack_info)

`pack` takes a data structure (which is assumed to be a Python `dict`)
containing information about how to handle the message at the level above
packing.  The packer simply needs to return a byte array (Python "bytes"
type) from the envelope.  It does this according to the `pack_info` argument,
which specifies the serialization format to be used.

Unpacker
--------
unpack(packet)

`unpack` takes a `packet` which is assumed to be a byte array (Python
`bytes`). The unpacker produces a data structure from the array, which
is assumed to be an `envelope` (a Python `dict`).

Packed format
-------------
The packed format consists of a header separated from a payload by a
partition string:

    <header><partition><payload>

The header is a JSON-encoded dict of information about the packing scheme
used in the payload.  Note that the packing scheme may not be JSON but
something different; JSON is just used to encode the header info.
"""
import json

# byte string used to partition header from payload
partition = b'||'

# holds all the possible packers
packers = {}


def pack(envelope, pack_info):
    """Pack envelope into a byte buffer.

    Parameters
    ----------
    envelope : data structure
    pack_info : packing information

    Returns
    -------
    packet : bytes

    """
    packer = get_packer(pack_info.ptype)
    payload = packer.pack(envelope)
    hdr = dict(packer=packer.kind, ver=packer.version,
               nbytes=len(payload))
    hdr_buf = json.dumps(hdr).encode()
    packet = hdr_buf + partition + payload
    return packet


def unpack(packet):
    """Unpack data from a byte buffer.

    Parameters
    ----------
    packet : bytes

    Returns
    -------
    envelope : data structure

    """
    hdr_buf, _, payload = packet.partition(partition)
    hdr = json.loads(hdr_buf)
    if len(payload) != hdr['nbytes']:
        raise ValueError("payload len (%d) does not match header (%d)" % (
            len(payload), hdr['nbytes']))

    packer = get_packer(hdr['packer'])
    envelope = packer.unpack(payload)
    return envelope


############################################################
# Try to import all the possible data packers
############################################################

#: Packers deliberately withdrawn, and why.  Named separately from "never
#: heard of it" so that a peer still sending one gets an answer an operator
#: can act on.
withdrawn_packers = {
    'pickle': "unpickling executes arbitrary code, so a service accepting it "
              "would run whatever anyone able to reach its port sent",
}


def get_packer(typ):
    """Look up a packer by name.

    :raises ValueError: if the name is withdrawn or unknown.
    """
    try:
        return packers[typ]
    except KeyError:
        pass

    if typ in withdrawn_packers:
        raise ValueError("the '%s' packer has been removed: %s"
                         % (typ, withdrawn_packers[typ]))
    raise ValueError("no packer named '%s'; available packers are %s"
                     % (typ, ', '.join(sorted(packers)) or '(none)'))


# msgpack
try:
    from .packers import pack_msgpack
    m = pack_msgpack.Packer()
    # TODO: need to handle versioning
    typ, _, ver = str(m).partition('/')
    packers[typ] = m

except ImportError:
    pass

# JSON
try:
    from .packers import pack_json
    m = pack_json.Packer()
    # TODO: need to handle versioning
    typ, _, ver = str(m).partition('/')
    packers[typ] = m

except ImportError:
    pass

# XML
try:
    from .packers import pack_xml
    m = pack_xml.Packer()
    # TODO: need to handle versioning
    typ, _, ver = str(m).partition('/')
    packers[typ] = m

except ImportError:
    pass

# END
