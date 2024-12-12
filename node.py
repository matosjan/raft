import os
import time
import random
import json
import threading
import requests
from flask import Flask, request, jsonify
import math
import asyncio

app = Flask(__name__)
    
class Node:
    def __init__(self, node_id, cluster):
        self.node_id = node_id
        self.cluster = cluster

        # Stored on disk
        self.curr_term = 0
        self.voted_for = None
        self.log = []
        self.commit_len = 0
        self.client_data = {}

        # Stored in RAM
        self.curr_role = 'follower'
        self.curr_leader = None
        self.votes_recieved = set()
        self.sent_len = dict()
        self.acked_len = dict()

        self.election_timeout = random.uniform(5, 10)
        self.last_heartbeat = time.time()
        self.heartbeat_interval = 3

        threading.Thread(target=self.election_timer, daemon=True).start()

    def election_timer(self):
        while True:
            if self.curr_role == "follower" and (time.time() - self.last_heartbeat > self.election_timeout):
                print(f'node {self.node_id}, time {time.time()}, heart {self.last_heartbeat}, diff {time.time() - self.last_heartbeat}')
                self.start_election()
            time.sleep(0.5)

    def send_heartbeat(self):
        while self.curr_role == "leader":
            for follower in self.cluster:
                if follower != self.node_id:
                    threading.Thread(target=self.replicate_log, args=(follower, True), daemon=True).start()
            time.sleep(self.heartbeat_interval)

    def send(self, to_id, msg_type, msg):
        # 12121 + node.leader_id
        port = 12121 + to_id
        url = f"http://node{to_id}:{port}/raft/{msg_type}"
        msg['node_id'] = self.node_id
        try:
            requests.post(url, json=msg)
        except requests.exceptions.RequestException as e:
            print(f'Node {to_id} maybe failed')

    def start_election(self):
        self.curr_term += 1
        self.curr_role = 'candidate'
        self.voted_for = self.node_id
        self.votes_recieved = set([self.node_id])
        
        last_log_term = 0
        if len(self.log) > 0:
            last_log_term = self.log[-1]['term']
        
        vote_request_msg = {
            'term': self.curr_term,
            'log_len': len(self.log),
            'last_log_term': last_log_term
        }
        for node_id in self.cluster:
            if node_id != self.node_id:
                self.send(to_id=node_id, msg_type='vote_request', msg=vote_request_msg)
        #start elcetion timer

    def recieve_vote_request(self, msg):
        c_id = msg['node_id']
        c_term = msg['term']
        c_log_term = msg['last_log_term']
        c_log_len = msg['log_len']

        my_log_term = 0
        if len(self.log) > 0:
            my_log_term = self.log[len(self.log) - 1]['term'] 

        log_ok = (c_log_term > self.curr_term) or (c_log_term == my_log_term and c_log_len >= len(self.log))

        term_ok = (c_term > self.curr_term) or (c_term == self.curr_term and self.voted_for in set([None, c_id]))

        vote_response_msg = None
        if log_ok and term_ok:
            self.curr_term = c_term
            self.curr_role = 'follower'
            self.voted_for = c_id
            vote_response_msg = {
                'term': self.curr_term,
                'granted': True,
            }
        else:
            vote_response_msg = {
                'term': self.curr_term,
                'granted': False,
            }
        self.send(to_id=c_id, msg_type='vote_response', msg=vote_response_msg) 

    def recieve_vote_response(self, msg):
        voter_id = msg['node_id']
        voter_term = msg['term']
        voter_voice = msg['granted']
        if self.curr_role == 'candidate' and self.curr_term == voter_term and voter_voice is True:
            self.votes_recieved.add(voter_id)
            if len(self.votes_recieved) >= math.ceil((len(cluster) + 1) / 2):
                self.curr_role = 'leader'
                print(f'Node {self.node_id} became leader')
                self.curr_leader = self.node_id
                # cancel election timer
                for follower_id in cluster:
                    if follower_id != self.node_id:
                        self.sent_len[follower_id] = len(self.log)
                        self.acked_len[follower_id] = 0
                        self.replicate_log(follower_id)
                threading.Thread(target=self.send_heartbeat, daemon=True).start()
            elif voter_term > self.curr_term:
                self.curr_term = voter_term
                self.curr_role = 'follower'
                self.voted_for = None
                # cancel election timer

    def broadcast(self, key, value, expected_value=None):
        if self.curr_role == 'leader':
            entry = {
                'term': self.curr_term,
                'key': key,
                'value': value
            }
            to_return = None
            if expected_value is not None:
                if self.client_data.get(key, None) == expected_value:
                    to_return = jsonify({"msg": "CAS successful"}), 200
                else:
                    # print(self.client_data.get(key, None))
                    return jsonify({"msg": "CAS unsuccessful"}), 200
                    
            self.log.append(entry)
            self.acked_len[self.node_id] = len(self.log)
            for follower_id in cluster:
                if follower_id != self.node_id:
                    self.replicate_log(follower_id)
            if to_return is not None:
                return to_return
        else:
            return jsonify({"msg": "Resend to leade", "leader_id": self.curr_leader}), 302

    def replicate_log(self, follower_id, heartbeat=False):
        i = self.sent_len[follower_id]
        entries = self.log[i:] if heartbeat == False else []
        prev_log_term = 0
        if i > 0:
            prev_log_term = self.log[i - 1]['term']

        log_request_msg = {
            'term': self.curr_term,
            'log_len': i,
            'log_term': prev_log_term,
            'commit_len': self.commit_len,
            'entries': entries
        }
        # print(entries)
        # if entries != []:
            # print(follower_id, log_request_msg)
        self.send(to_id=follower_id, msg_type='log_request', msg=log_request_msg)

    def recieve_log_request(self, msg):
        if msg['term'] > self.curr_term:
            self.curr_term = msg['term']
            self.voted_for = None
            self.curr_role = 'follower'
            self.curr_leader = msg['node_id'] # leader_id
        
        if msg['term'] == self.curr_term and self.curr_role == 'candidate':
            self.curr_role = 'follower'
            self.curr_leader = msg['node_id']
        
        log_ok = (len(self.log) >= msg['log_len']) and (msg['log_len'] == 0 or msg['log_term'] == self.log[msg['log_len'] - 1]['term'])

        log_response_msg = None
        if msg['term'] == self.curr_term and log_ok:
            self.append_entries(msg)
            ack_len = msg['log_len'] + len(msg['entries'])
            log_response_msg = {
                'term': self.curr_term,
                'ack_len': ack_len,
                'log_success': True
            }
        else:
            log_response_msg = {
                'term': self.curr_term,
                'ack_len': 0,
                'log_success': True
            }
        self.send(to_id=msg['node_id'], msg_type='log_response', msg=log_response_msg)

    def append_entries(self, msg):
        entries = msg['entries']
        if entries == []:
            print(f"heartbeat from {msg['node_id']} on {self.node_id}")
            self.last_heartbeat = time.time()
            # return
        
        leader_log_len = msg['log_len']
        leader_commit_len = msg['commit_len']
        if len(entries) > 0 and len(self.log) > leader_log_len:
            self.log = self.log[:leader_log_len]
        
        if leader_log_len + len(entries) > len(self.log):
            for i in range(len(self.log) - leader_log_len, len(entries)):
                self.log.append(entries[i])
        
        # print(f'on {self.node_id} lead len{leader_commit_len}, {self.commit_len}')
        if leader_commit_len > self.commit_len:
            for i in range(self.commit_len, leader_commit_len):
                self.deliver_to_kv(self.log[i])
            # print(f'node {self.node_id}, commitlen {self.commit_len}')
            self.commit_len = leader_commit_len
        # print(f'Log on node {self.node_id}: {self.log}')
        
    def recieve_log_response(self, msg):
        follower_id = msg['node_id']
        # print(msg)
        if msg['term'] == self.curr_term and self.curr_role == 'leader':
            if msg['log_success'] == True and msg['ack_len'] >= self.acked_len[follower_id]:
                self.sent_len[follower_id] = msg['ack_len']
                self.acked_len[follower_id] = msg['ack_len']
                self.commit_log_entries()
            elif self.sent_len[follower_id] > 0:
                self.sent_len[follower_id] = self.sent_len[follower_id] - 1
                self.replicate_log(follower_id)
        elif msg['term'] > self.curr_term:
            self.curr_term = msg['term']
            self.curr_role = 'follower'
            self.voted_for = None

    def deliver_to_kv(self, entry):
        key = entry['key']
        value = entry['value']
        if value == None:
            self.client_data.pop(key)
        else:
            self.client_data[key] = value
        print(f'Client data on node {self.node_id}')

    def acks(self, len):
        res = set()
        for node in self.cluster:
            if self.acked_len[node] >= len:
                res.add(node)
        return res

    def commit_log_entries(self):
        min_acks = math.ceil((len(self.cluster) + 1) / 2)
        ready = set()
        for l in range(1, len(self.log) + 1):
            if len(self.acks(l)) >= min_acks:
                ready.add(l)
        # print(ready, self.commit_len, self.node_id)

        if len(ready) != 0 and max(ready) > self.commit_len and self.log[max(ready) - 1]['term'] == self.curr_term:
            for i in range(self.commit_len, max(ready)):
                self.deliver_to_kv(self.log[i])
            self.commit_len = max(ready)


@app.route('/raft/log_request', methods=['POST'])
def http_log_request():
    msg = request.get_json()
    node.recieve_log_request(msg)
    return '', 204

@app.route('/raft/vote_request', methods=['POST'])
def http_request_vote():
    msg = request.get_json()
    node.recieve_vote_request(msg)
    return '', 204

@app.route('/raft/vote_response', methods=['POST'])
def http_vote_response():
    msg = request.get_json()
    node.recieve_vote_response(msg)
    return '', 204

@app.route('/raft/log_response', methods=['POST'])
def http_log_response():
    msg = request.get_json()
    node.recieve_log_response(msg)
    return '', 204

@app.route('/kv/<key>', methods=['GET'])
def client_get(key):
    return jsonify({"key": key, "value": node.client_data.get(key, None)})
 
@app.route('/kv/<key>', methods=['PUT'])
def client_put(key):
    value = request.get_json()['value']
    response = node.broadcast(key, value)
    return response

@app.route('/kv/<key>', methods=['PATCH'])
def client_update(key):
    value = request.get_json()['value']
    expected_value = request.get_json().get('exp_value', None)
    response = node.broadcast(key, value, expected_value)
   
    return response

@app.route('/kv/<key>', methods=['DELETE'])
def client_delete(key):
    response = node.broadcast(key, None)
    return response

if __name__ == "__main__":
    import sys
    node_id = int(sys.argv[1])
    cluster = [0, 1, 2, 3]
    node = Node(node_id, cluster)
    app.run(host="0.0.0.0", port=12121 + node_id)
