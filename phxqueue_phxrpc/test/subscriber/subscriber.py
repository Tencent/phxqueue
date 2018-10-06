#!/usr/bin/env python

from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from optparse import OptionParser

import sys
sys.path.insert(0, './phxqueue/comm/proto')
import comm_pb2

class RequestHandler(BaseHTTPRequestHandler):

    def do_GET(self):

        request_path = self.path

        print("\n----- Request Start ----->\n")
        print(request_path)
        print(self.headers)
        print("<----- Request End -----\n")

        self.send_response(404)

    def do_POST(self):

        request_path = self.path

        print("\n----- Request Start ----->\n")
        print("request_path %s" % request_path)

        request_headers = self.headers
        content_length = request_headers.getheaders('content-length')
        length = int(content_length[0]) if content_length else 0
        content = self.rfile.read(length) # <--- Gets the data itself


        if request_path == "/push":
            push_req = comm_pb2.PushRequest()
            push_req.ParseFromString(content[:length])

            print("topic_id %d" % push_req.topic_id)
            print("pub_id %d" % push_req.pub_id)
            print("client_id %s" % push_req.client_id)
            print("count %u" % push_req.count)
            print("atime %u" % push_req.atime)
            print("buffer %s" % push_req.buffer)

            push_resp = comm_pb2.PushResponse()
            push_resp.result = "success"
            response_content = push_resp.SerializeToString()

            self.send_response(200)
            self.send_header("Content-Length", str(len(response_content)))
            self.end_headers()
            self.wfile.write(response_content)
        elif request_path == "/txquery":
            txquery_req = comm_pb2.TxQueryRequest()
            txquery_req.ParseFromString(content[:length])

            print("topic_id %d" % txquery_req.topic_id)
            print("pub_id %d" % txquery_req.pub_id)
            print("client_id %s" % txquery_req.client_id)
            print("count %u" % txquery_req.count)
            print("atime %u" % txquery_req.atime)
            print("buffer %s" % txquery_req.buffer)

            txquery_resp = comm_pb2.TxQueryResponse()
            if txquery_req.count >= 10:
                txquery_resp.status_info.tx_status = comm_pb2.TX_COMMIT
            else:
                txquery_resp.status_info.tx_status = comm_pb2.TX_UNCERTAIN
            response_content = txquery_resp.SerializeToString()

            self.send_response(200)
            self.send_header("Content-Length", str(len(response_content)))
            self.end_headers()
            self.wfile.write(response_content)
        else:
            self.send_response(400)


        print("<----- Request End -----\n")



    do_PUT = do_POST
    do_DELETE = do_GET

def main():
    port = 8081
    print('Listening on localhost:%s' % port)
    server = HTTPServer(('', port), RequestHandler)
    server.serve_forever()


if __name__ == "__main__":
    parser = OptionParser()
    parser.usage = ("Creates an http-server that will echo out any GET or POST parameters\n"
                    "Run:\n\n"
                    "   reflect")
    (options, args) = parser.parse_args()

    main()
