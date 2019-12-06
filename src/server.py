from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
from hashlib import sha256
import argparse

import xmlrpc.client
import threading
import random
import time

import signal


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

    # return only if it is the leader server
    if currentState == 'leader':
        commit = False
        while not commit:
            ack = 1
            for node in nodelist:
                if not node.surfstore.isCrashed():
                    ack += 1
                    print("# of nodes not crashed ", ack)
                    if ack > maxnum / 2:
                        commit = True
                        break
        return fileinfomap

    else:
        raise Exception("This is not the Leader Server. Get file failed")


# Update a file's fileinfo entry
def updatefile(filename, version, hashlist):
    """Updates a file's fileinfo entry"""

    global log

    print("UpdateFile(" + filename + ")")

    if crashFlag:
        raise Exception("Leader server crashed")

    if currentState == 'leader':

        response = True
        if filename not in fileinfomap:
            log.append((currentTerm, (filename, [1, hashlist])))
        else:
            currentVersion = fileinfomap[filename][0]
            if version == currentVersion + 1:
                log.append((currentTerm, (filename, [version, hashlist])))
            else:
                log.append((currentTerm, (filename, [currentVersion,
                                                     hashlist])))
                response = False

        shouldApply = len(log) - 1
        print("Updated Log: ", log)

        while lastApplied < shouldApply:
            pass

        # respond after entry applied to state machine
        return response

    else:
        raise Exception("This is not the Leader Server. Update file failed")


# PROJECT 3 APIs below


# Queries whether this metadata store is a leader
# Note that this call should work even when the server is "crashed"
def isLeader():
    global currentState
    """Is this metadata store a leader?"""
    print("IsLeader()")
    if currentState == 'leader':
        return True
    return False


# "Crashes" this metadata store
# Until Restore() is called, the server should reply to all RPCs
# with an error (unless indicated otherwise), and shouldn't send
# RPCs to other servers
def crash():
    global crashFlag
    global timer
    global currentState
    """Crashes this metadata store"""
    print("Crash()")
    if not crashFlag:
        crashFlag = True
        currentState = 'follower'
        if timer.isAlive():
            timer.cancel()
    return


# "Restores" this metadata store, allowing it to start responding
# to and sending RPCs to other nodes
def restore():
    global crashFlag
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
    global timerFreset
    global votedFor

    print("RequestVote from {}, term {}, currentTerm {}, votedFor {}.".format(
        candidateId, term, currentTerm, votedFor))

    if crashFlag:
        raise Exception("isCrashed!")

    if term > currentTerm:
        currentTerm = term
        votedFor = None
        if currentState == 'follower':
            timerFreset = True
        else:
            currentState = 'follower'

    if term < currentTerm:
        return False

    # If votedFor is null or candidateId, and candidate’s log is at
    # least as up-to-date as receiver’s log, grant vote
    if votedFor is None or votedFor == candidateId:
        if lastLogTerm > log[-1][0] or (lastLogTerm == log[-1][0]
                                        and lastLogIndex >= len(log) - 1):
            votedFor = candidateId
            return True
    return False


# Updates fileinfomap
def appendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries,
                  leaderCommit):
    """Updates fileinfomap to match that of the leader"""
    global log
    global currentState
    global currentTerm
    global commitIndex
    global timerFreset
    global votedFor

    print("AppendEntries from {}.".format(leaderId))

    if crashFlag:
        raise Exception("isCrashed!")

    if term > currentTerm:
        currentTerm = term
        votedFor = None

    if currentState == 'candidate' or currentState == 'leader':
        currentState = 'follower'
    else:
        timerFreset = True

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
        print(commitIndex)
    return True


def tester_getversion(filename):
    return None if filename not in fileinfomap else fileinfomap[filename][0]


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
def run_leader():
    global commitIndex
    global currentState
    global lastApplied
    global fileinfomap
    global timer

    print("Running Leader")
    if timer.isAlive():
        timer.cancel()
    # local variable (reinitialized after election)
    # nextIndex initialized to leader last log index + 1
    nextIndex = [len(log) for i in range(len(nodelist))]
    # matchIndex initialized to 0
    matchIndex = [0 for i in range(len(nodelist))]

    # send initial empty AppendEntries RPCs (heartbeat)
    while currentState == 'leader':
        for idx, node in enumerate(nodelist):
            lastLogIndex = len(log) - 1
            if lastLogIndex >= nextIndex[idx]:
                try:
                    if node.surfstore.appendEntries(
                            currentTerm, servernum, nextIndex[idx] - 1,
                            log[nextIndex[idx] - 1][0],
                            log[nextIndex[idx]:lastLogIndex + 1], commitIndex):
                        nextIndex[idx] = lastLogIndex + 1
                        matchIndex[idx] = lastLogIndex
                    else:
                        nextIndex[idx] -= 1
                except Exception as e:
                    # print("Error occurred: ", e)
                    pass

            else:
                try:
                    #print("Sending hb to ", node, "from", servernum)
                    node.surfstore.appendEntries(currentTerm, servernum,
                                                 nextIndex[idx] - 1,
                                                 log[nextIndex[idx] - 1][0],
                                                 [], commitIndex)
                except:
                    pass

        for n in range(len(log) - 1, commitIndex, -1):
            if sum([i >= n for i in matchIndex
                    ]) + 1 > maxnum / 2 and log[n][0] == currentTerm:
                commitIndex = n

        while commitIndex > lastApplied:
            lastApplied += 1
            filename, fileinfo = log[lastApplied][1]
            print("add {} to fileinfomap".format(filename))
            fileinfomap[filename] = fileinfo

        # periodically send
        time.sleep(0.1)

    if currentState == 'follower':
        run_follower()


# dummy heartbeat
def heartbeat(server):
    print("Recevied HeartBeat from", server)


# follower rules
def run_follower():
    global timerFreset
    global currentState
    global timer
    global lastApplied

    while crashFlag:
        pass

    while commitIndex > lastApplied:
        lastApplied += 1
        filename, fileinfo = log[lastApplied][1]
        fileinfomap[filename] = fileinfo
        print("fileinfomap updated: ", filename, fileinfo[0])

    print("Running Follower, currentTerm is ", currentTerm)
    if timer.isAlive():
        timer.cancel()
    timer = threading.Timer(random.randint(700, 1000) / 1000, run_candidate)
    timer.start()
    while timer.isAlive():
        if timerFreset:
            timer.cancel()
            timerFreset = False
            run_follower()


# candidate rules
def run_candidate():
    global currentTerm
    global currentState
    global votedFor
    global timer

    currentTerm += 1
    votedFor = servernum
    print("Running Candidate, currentTerm: ", currentTerm)
    currentState = 'candidate'

    if timer.isAlive():
        timer.cancel()
    timer = threading.Timer(random.randint(700, 1000) / 1000, run_candidate)
    timer.start()
    # send requestVote RPCs to all other servers
    voteCount = 1  # initial vote from itself
    for node in nodelist:
        try:
            if node.surfstore.requestVote(currentTerm, servernum,
                                          len(log) - 1, log[-1][0]):
                print("Receive vote from ", node)
                voteCount += 1
        except:
            pass
    if voteCount > maxnum / 2 and crashFlag == False:
        currentState = 'leader'

    while timer.isAlive() or crashFlag:
        if currentState == 'leader':
            timer.cancel()
            run_leader()
            break
        elif currentState == 'follower':
            timer.cancel()
            run_follower()
            break
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
        print("Maxnum number of servers is: ", maxnum)

        hashmap = dict()

        fileinfomap = dict()

        crashFlag = False
        currentState = 'follower'
        timerFreset = False
        # persistent state on all servers
        currentTerm = 0
        votedFor = None
        log = [(0, None)]  # (term, command)
        # Volatile state on all servers
        commitIndex = 0
        lastApplied = 0

        print("Attempting to start XML-RPC Server...")
        print(host, port)
        server = threadedXMLRPCServer((host, port),
                                      requestHandler=RequestHandler,
                                      allow_none=True)
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

        # dummy heartbeat rpc
        server.register_function(heartbeat, "surfstore.heartbeat")

        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")

        # server.serve_forever()

        nodelist = [
            xmlrpc.client.ServerProxy("http://" + i) for i in serverlist
        ]
        t1 = threading.Thread(target=server.serve_forever)
        t1.start()

        timer = threading.Timer(
            random.randint(700, 1000) / 1000, run_candidate)

        # # the main process
        run_follower()

    except Exception as e:
        print("Server: " + str(e))
