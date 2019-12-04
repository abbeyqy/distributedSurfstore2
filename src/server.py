from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from hashlib import sha256
import argparse


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2', )


class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass


# A simple ping, returns true
def ping():
    """A simple ping method"""
    print("Ping()")
    return True


# Gets a block, given a specific hash value
def getblock(h):
    """Gets a block"""
    print("GetBlock(" + h + ")")

    blockData = bytes(4)
    return blockData


# Puts a block
def putblock(b):
    """Puts a block"""
    print("PutBlock()")

    return True


# Given a list of hashes, return the subset that are on this server
def hasblocks(hashlist):
    """Determines which blocks are on this server"""
    print("HasBlocks()")

    return hashlist


# Retrieves the server's FileInfoMap
def getfileinfomap():
    """Gets the fileinfo map"""
    print("GetFileInfoMap()")

    return fileinfomap


# Update a file's fileinfo entry
def updatefile(filename, version, hashlist):
    """Updates a file's fileinfo entry"""
    print("UpdateFile(" + filename + ")")

    fileinfomap[filename] = [version, hashlist]

    return True


# PROJECT 3 APIs below


# Queries whether this metadata store is a leader
# Note that this call should work even when the server is "crashed"
def isLeader():
    """Is this metadata store a leader?"""
    print("IsLeader()")
    return


# "Crashes" this metadata store
# Until Restore() is called, the server should reply to all RPCs
# with an error (unless indicated otherwise), and shouldn't send
# RPCs to other servers
def crash():
    """Crashes this metadata store"""
    print("Crash()")
    if not crashFlag:
        crashFlag = True
    return


# "Restores" this metadata store, allowing it to start responding
# to and sending RPCs to other nodes
def restore():
    """Restores this metadata store"""
    print("Restore()")
    if crashFlag:
        crashFlag = False
    return


# "IsCrashed" returns the status of this metadata node (crashed or not)
# This method should always work, even when the node is crashed
def isCrashed():
    """Returns whether this node is crashed or not"""
    print("IsCrashed()")
    return crashFlag


# Requests vote from this server to become the leader
def requestVote(log, term, candidateId, lastLogIndex, lastLogTerm):
    """Requests vote to be the leader"""
    if term < currentTerm:
        return False

    # If votedFor is null or candidateId, and candidate’s log is at
    # least as up-to-date as receiver’s log, grant vote
    if votedFor is None or votedFor == candidateId:
        if lastLogTerm > log[-1][0] or (lastLogTerm == log[-1][0]
                                        and lastLogIndex >= len(log) - 1):
            return True
    return False


# Updates fileinfomap
def appendEntries(log, term, leaderId, prevLogIndex, prevLogTerm, entries,
                  leaderCommit):
    """Updates fileinfomap to match that of the leader"""
    if term < currentTerm:
        return False
    if log[prevLogIndex][0] != prevLogTerm:
        return False
    for idx in range(prevLogIndex + 1, len(log)):
        if log[idx][0] != entries[idx - prevLogIndex - 1][0]:
            log = log[:idx]
            log += entries[idx - prevLogIndex - 1:]
            break
    if leaderCommit > commitIndex:
        commitIndex = min(leaderCommit, len(log) - 1)
    return True


def tester_getversion(filename):
    return fileinfomap[filename][0]


# Reads the config file and return host, port and store list of other servers
def readconfig(config, servernum):
    """Reads cofig file"""
    fd = open(config, 'r')
    l = fd.readline()

    maxnum = int(l.strip().split(' ')[1])

    if servernum >= maxnum or servernum < 0:
        raise Exception('Server number out of range.')

    d = fd.read()
    d = d.splitlines()

    for i in range(len(d)):
        hostport = d[i].strip().split(' ')[1]
        if i == servernum:
            host = hostport.split(':')[0]
            port = int(hostport.split(':')[1])

        else:
            serverlist.append(hostport)

    return maxnum, host, port


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="SurfStore server")
        parser.add_argument('config', help='path to config file')
        parser.add_argument('servernum', type=int, help='server number')

        args = parser.parse_args()

        config = args.config
        servernum = args.servernum

        # server list has list of other servers
        serverlist = []

        # maxnum is maximum number of servers
        maxnum, host, port = readconfig(config, servernum)

        hashmap = dict()

        fileinfomap = dict()

        crashFlag = False
        # persistent state on all servers
        currentTerm = 0
        votedFor = None
        log = []  # (term, command)
        # Volatile state on all servers
        commitIndex = 0
        lastApplied = 0

        print("Attempting to start XML-RPC Server...")
        print(host, port)
        server = threadedXMLRPCServer((host, port),
                                      requestHandler=RequestHandler)
        server.register_introspection_functions()
        server.register_function(ping, "surfstore.ping")
        server.register_function(getblock, "surfstore.getblock")
        server.register_function(putblock, "surfstore.putblock")
        server.register_function(hasblocks, "surfstore.hasblocks")
        server.register_function(getfileinfomap, "surfstore.getfileinfomap")
        server.register_function(updatefile, "surfstore.updatefile")
        # Project 3 APIs
        server.register_function(isLeader, "surfstore.isLeader")
        server.register_function(crash, "surfstore.crash")
        server.register_function(restore, "surfstore.restore")
        server.register_function(isCrashed, "surfstore.isCrashed")
        server.register_function(requestVote, "surfstore.requestVote")
        server.register_function(appendEntries, "surfstore.appendEntries")
        server.register_function(tester_getversion,
                                 "surfstore.tester_getversion")
        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")
        server.serve_forever()
    except Exception as e:
        print("Server: " + str(e))
