from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from hashlib import sha256
import argparse

import xmlrpc.client
import threading
import random
import time


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
def requestVote(term, candidateId, lastLogIndex, lastLogTerm):
    """Requests vote to be the leader"""
    global currentState
    global currentTerm

    if term > currentTerm:
        currentTerm = term
        currentState = 'follower'

    if term < currentTerm:
        return False

    # If votedFor is null or candidateId, and candidate’s log is at
    # least as up-to-date as receiver’s log, grant vote
    if votedFor is None or votedFor == candidateId:
        if lastLogTerm > log[-1][0] or (lastLogTerm == log[-1][0] and lastLogIndex >= len(log) - 1):
            return True
    return False


# Updates fileinfomap
def appendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries,
                  leaderCommit):
    """Updates fileinfomap to match that of the leader"""
    global log
    global currentState
    global currentTerm

    if term > currentTerm:
        currentTerm = term

    if currentState == 'candidate' or currentState == 'leader':
        currentState = 'follower'

    #1. reply false if term < currentTerm
    if term < currentTerm:
        return False
    #2. reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm.
    if log[prevLogIndex][0] != prevLogTerm:
        return False
    #3. If an existing entry conflicts with a new one(same index different term),
    # delete the existing entry and all that follow it.
    #4. Append any new entries not already in the log
    append_flag = False
    for idx in range(prevLogIndex + 1, len(log)):
        if log[idx][0] != entries[idx - prevLogIndex - 1][0]:
            log = log[:idx]
            log += entries[idx - prevLogIndex - 1:]
            append_flag = True
            break
    if not append_flag:
        log += entries[len(log) - prevLogIndex - 1:]
    #5. If leaderCommit > commitIndex, set commitIndex=min(leaderCommit, index of last new entry)
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


# leader behaviors
def run_leader(nodelist, server):
    print("Running Leader")

    # send initial empty AppendEntries RPCs (heartbeat)
    while True:
        for node in nodelist:
            print("Sending hb to ", node, "from", server)
            node.surfstore.heartbeat(server)

            # periodically send
            time.sleep(1)

# dummy heatbeat
def heartbeat(server):
    print("Recevied HeartBeat from", server)


# follower rules
def run_follower():
    print("Running Follower")
    timer.start()


# candidate rules
def run_candidate():
    global currentTerm
    global currentState

    print("Running Candidate")
    currentTerm += 1
    timer.cancel()
    timer.start()
    # send requestVote RPCs to all other servers
    voteCount = 1  # initial vote from itself
    for node in nodelist:
        if node.surfstore.requestVote(currentTerm, servernum, len(log) - 1, log[-1][0]):
            voteCount += 1
    if voteCount > maxnum / 2:
        currentState = 'leader'
    return


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
        currentState = 'follower'
        # persistent state on all servers
        currentTerm = 0
        votedFor = None
        log = []  # (term, command)
        # Volatile state on all servers
        commitIndex = 0
        lastApplied = 0

        print("Attempting to start XML-RPC Server...")
        print(host, port)
        server = threadedXMLRPCServer((host, port),requestHandler=RequestHandler, allow_none=True)
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
        server.register_function(tester_getversion,"surfstore.tester_getversion")

        # dummy heartbeat rpc
        server.register_function(heartbeat, "surfstore.heartbeat")

        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")

        # server.serve_forever()

        nodelist = [xmlrpc.client.ServerProxy("http://" + i) for i in serverlist]
        t1 = threading.Thread(target=server.serve_forever)
        t1.start()

        nodelist = [xmlrpc.client.ServerProxy("http://" + i) for i in serverlist]

        # # the main process
        # timer = threading.Timer(random.randint(200, 800) / 1000, run_candidate)
        # run_follower()

        # test case, when servernum == 0, it is leader, else follower
        if servernum == 0:
            run_leader(nodelist, servernum)

    except Exception as e:
        print("Server: " + str(e))
