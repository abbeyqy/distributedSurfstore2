import argparse
import xmlrpc.client

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="SurfStore client")
    parser.add_argument('leader', help='host:port of the server')
    parser.add_argument('hostport', help='host:port of the server')
    args = parser.parse_args()

    hostport = args.hostport
    leader = args.leader

    try:
        leader = xmlrpc.client.ServerProxy('http://' + leader)
        client = xmlrpc.client.ServerProxy('http://' + hostport)
        # Test ping
        client.surfstore.ping()
        print("Ping() successful")
        print("after isLeader: ", client.surfstore.isLeader())
        print("isCrashed: ", client.surfstore.isCrashed())

        print("crash: ", hostport, client.surfstore.crash())

        print("updatefile: ",
              leader.surfstore.updatefile("test.txt", 1, [1, 2, 3]))
        print("crashed node getversion: ", client.surfstore.tester_getversion("test.txt"))
        print("leader version number: ",
              leader.surfstore.tester_getversion("test.txt"))

    except Exception as e:
        print("Client: " + str(e))
