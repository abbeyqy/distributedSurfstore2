import argparse
import xmlrpc.client

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="SurfStore client")
    parser.add_argument('hostport', help='host:port of the server')
    args = parser.parse_args()

    hostport = args.hostport

    try:
        client = xmlrpc.client.ServerProxy('http://' + hostport)
        # Test ping
        client.surfstore.ping()
        print("Ping() successful")
        print("after isLeader: ", client.surfstore.isLeader())
        print("isCrashed: ", client.surfstore.isCrashed())
        #client.surfstore.crash()
        #client.surfstore.restore()
        print("updatefile: ",client.surfstore.updatefile("test.txt", 1, [1, 2, 3]))
        print("getversion: ", client.surfstore.tester_getversion("test.txt"))

    except Exception as e:
        print("Client: " + str(e))
