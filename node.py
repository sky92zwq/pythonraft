from asyncio import futures
from enum import Enum
import time

import grpc

import raft_pb2
import raft_pb2_grpc
import logging
import argparse

class Role(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

import asyncio
import random
from raft_pb2 import RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse, LogEntry

class DelayedTrigger:
    def __init__(self, delay: int, callback):
        self.delay = delay
        self.callback = callback
        self._task = None

    async def trigger(self):
        try:
            print(f"Waiting for {self.delay} seconds...")
            # 如果任务被取消，跳出 sleep
            await asyncio.sleep(self.delay)
            await self.callback()
        except asyncio.CancelledError:
            print("Task was cancelled before it completed.")

    def start(self):
        # 启动延时触发器任务
        self._task = asyncio.create_task(self.trigger())

    def cancel(self):
        if self._task:
            self._task.cancel()  # 取消任务



class Node(raft_pb2_grpc.RaftServicer):
    def __init__(self):
        self.currentTerm = 0
        self.votedReceived = 0
        self.votedFor = None

        self.commitIndex = 0
        self.log = []
        self.peers = []

        self.followerTimer = None
        self.leaderTimer = None

        self.id = None
        asyncio.create_task(self.set_role(Role.FOLLOWER))

    async def set_role(self, role):
        if isinstance(role, Role):
            self.role = role
            if self.role == Role.LEADER:
                await self.heartbeat()
            elif self.role == Role.CANDIDATE:
                await self.leaderElection()
            elif self.role == Role.FOLLOWER:
                await self.handleHeartbeat()
        else:
            raise await ValueError("Invalid role")

    async def RequestVote(self, request, context):
        logging.info(f"Node {self.id} received vote request {request}")
        response = raft_pb2.RequestVoteResponse()
   

        if request.term < self.currentTerm:
            response.voteGranted = False
            return  response

        if request.term > self.currentTerm:
            self.currentTerm = request.term
            self.votedFor = None

        if self.votedFor is None or self.votedFor == request.candidateId:
            self.votedFor = request.candidateId
            response.voteGranted = True
        else:
            response.voteGranted = False
        return  response

    async def leaderElection(self):
        logging.info(f"Node {self.id} starts election")

        time.sleep(random.randint(5, 15))
        logging.info(f"Node {self.id} starts election end")
        self.currentTerm += 1
        self.votedFor = None
        logging.info(f"Node {self.id} peers {self.peers}")
        for peer in self.peers:
            logging.info(f"Node {self.id} peer {peer}")
            request = RequestVoteRequest()
            request.candidateId = self.id
            request.term = self.currentTerm
            request.lastLogIndex = len(self.log) - 1
            if len(self.log) > 0: 
                request.lastLogTerm = self.log[-1].term
            else:
                request.lastLogTerm = 0
            logging.info(f"Node {self.id} create vote request {request}")

            response = await peer.RequestVote(request)
            logging.info(f"Node {self.id} received vote response {response}")
            if response.voteGranted:
                self.votedReceived += 1
            if self.votedReceived > len(self.peers) / 2:
                self.leaderTimer = DelayedTrigger(3, self.heartbeat)
                logging.info(f"Node {self.id} becomes leader")
                await self.set_role(Role.LEADER)
                return
                
    async def heartbeat(self):
        logging.info(f"Node {self.id} sends heartbeat")
        for peer in self.peers:
            request = AppendEntriesRequest()
            response = await peer.AppendEntries(request)
        self.leaderTimer.start()

    async def handleHeartbeat(self):
        tmp =self.followerTimer
        self.followerTimer = DelayedTrigger(10, self.handleHeartbeatTimeout)
        self.followerTimer.start()
        if tmp:
            tmp.cancel()

    async def handleHeartbeatTimeout(self):
        logging.info(f"Node {self.id} timeout")
        await self.set_role(Role.CANDIDATE)
        

    async def AppendEntries(self, request, context):
        logging.info(f"Node {self.id} received append entries {request}")
        response = AppendEntriesResponse()
        if request.term < self.currentTerm:
            response.success = False
            return  response

        self.currentTerm = request.term
        self.votedFor = None
        if request.entries is None and request.leaderId != self.id:
            response.success = True
            logging.info(f"Node {self.id} becomes follower")
            await self.set_role(Role.FOLLOWER)
            return  response

        if len(self.log) < request.prevLogIndex or self.log[request.prevLogIndex].term != request.prevLogTerm:
            response.success = False
            return  response

        self.log = self.log[:request.prevLogIndex + 1]
        self.log.extend(request.entries)

        if request.leaderCommit > self.commitIndex:
            self.commitIndex = min(request.leaderCommit, len(self.log) - 1)

        response.success = True
        return  response


    def write(self, data):
        if self.role != Role.LEADER:
            return False

        entry = LogEntry()
        entry.term = self.currentTerm
        entry.data = data
        appendCnt = 1

        for peer in self.peers:
            request = AppendEntriesRequest()
            request.term = self.currentTerm
            request.entries = [entry]
            request.leaderCommit = self.commitIndex
            request.leaderId = self.id
            response = AppendEntriesResponse()
            response = peer.AppendEntries(request)
            if response.success:
                appendCnt += 1
        if appendCnt > len(self.peers) / 2:
            self.commitIndex += 1
            self.log.append(entry)
        else:
            return False
        return True
    
async def channel_ready(channel):
    try:
        await channel.channel_ready()
    except grpc.RpcError as e:
        logging.error(f"Channel not ready: {e}")
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
    before_sleep_log
)

logger = logging.getLogger(__name__)

@retry(
    wait=wait_exponential(multiplier=1, max=2),  # 指数退避，最大间隔10秒
    stop=stop_after_attempt(5),                   # 最多重试5次
    retry=retry_if_exception_type(grpc.RpcError), # 仅重试 gRPC 错误
    before_sleep=before_sleep_log(logger, logging.WARNING)  # 重试前打印日志
)

async def serve():
    parser = argparse.ArgumentParser(description="Raft Node")
    parser.add_argument("--id", type=int, help="Node ID", required=True)
    parser.add_argument("--peers", type=lambda s: s.split(','), help="Peer addrs", required=True)
    parser.add_argument("--port", type=int, help="Port", default=5003)

    node = Node()
    node.id = parser.parse_args().id
    print("Node started {}".format(parser.parse_args().id))
    server = grpc.aio.server()
    raft_pb2_grpc.add_RaftServicer_to_server(node, server)
    server.add_insecure_port("localhost:"+(str(parser.parse_args().port)))
    logging.info("gRPC Server running on port {}...".format(parser.parse_args().port))
    await server.start()
    print("Node started {}".format(parser.parse_args().peers))
    print("Node started {}".format(parser.parse_args().port))
    for peer in parser.parse_args().peers:
        peer = grpc.aio.insecure_channel(peer)
        await channel_ready(peer)
        stub = raft_pb2_grpc.RaftStub(peer)
        node.peers.append(stub)
        logging.info(f"Node {node.id} connected to peer {peer} and stub is {stub}")

    await server.wait_for_termination()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(serve())