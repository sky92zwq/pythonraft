from asyncio import futures
from enum import Enum
import time

import grpc
import fastapi
import uvicorn

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
            logger.info(f"start a timer Waiting for {self.delay} seconds...")
            # 如果任务被取消，跳出 sleep
            await asyncio.sleep(self.delay)
            await self.callback()
        except asyncio.CancelledError:
            logger.error("Task was cancelled before it completed.")

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
        self.channels = []

        self.followerTimer = None
        self.leaderTimer = None

        self.id = None
        self.role = Role.FOLLOWER
        asyncio.create_task(self.set_role(Role.FOLLOWER))

        self.app = fastapi.FastAPI()
        self.router = fastapi.APIRouter()
        
        self.router.get("/all")(self.getAll)  # 绑定成员函数
        self.app.include_router(self.router)   # 注册路由


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
        logger.info(f"Node {self.id} received vote request term:{request.term} candidate:{request.candidateId} "
                      f"lastLogIndex:{request.lastLogIndex} lastLogTerm:{request.lastLogTerm}")
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
        logger.info(f"Node {self.id} starts election")

        await asyncio.sleep(random.randint(5, 15))
        logger.info(f"Node {self.id} starts election end")
        self.currentTerm += 1
        self.votedReceived = 0
        self.votedFor = None
        logger.info(f"Node {self.id} peers {self.peers}")
        for peer in self.peers:
            logger.info(f"Node {self.id} peer {peer}")
            request = RequestVoteRequest()
            request.candidateId = self.id
            request.term = self.currentTerm
            request.lastLogIndex = len(self.log) - 1
            if len(self.log) > 0: 
                request.lastLogTerm = self.log[-1].term
            else:
                request.lastLogTerm = 0
            logger.info(f"Node {self.id} create vote request {request}")

            response = await peer.RequestVote(request, wait_for_ready = True)
            logger.info(f"Node {self.id} received vote response {response}")
            if response.voteGranted:
                self.votedReceived += 1
            if self.votedReceived > len(self.peers) / 2:
                self.leaderTimer = DelayedTrigger(3, self.heartbeat)
                logger.info(f"Node {self.id} becomes leader")
                await self.set_role(Role.LEADER)
                return
                
    async def heartbeat(self):
        logger.info(f"Node {self.id} sends heartbeat")
        request = AppendEntriesRequest()
        request.term = self.currentTerm
        request.leaderId = self.id
        if len(self.log) > 0:
            request.prevLogIndex = len(self.log) -1
            request.prevLogTerm = self.log[-1].term
        request.leaderCommit = self.commitIndex
        logger.info(f"Node {self.id} sends heartbeat is {request}")
        for peer in self.peers:
            try:
                response = await peer.AppendEntries(request, wait_for_ready = False)
                logger.info(f"Node {self.id} sends heartbeat got {response}")
            except grpc.aio.AioRpcError as rpc_error:
                logger.error(f"Node {self.id} sends heartbeat got exception {rpc_error}")
        self.leaderTimer.start()

    async def handleHeartbeat(self):
        tmp =self.followerTimer
        self.followerTimer = DelayedTrigger(10, self.handleHeartbeatTimeout)
        self.followerTimer.start()
        if tmp:
            tmp.cancel()

    async def handleHeartbeatTimeout(self):
        logger.info(f"Node {self.id} timeout")
        await self.set_role(Role.CANDIDATE)
        

    async def AppendEntries(self, request, context):
        logger.info(f"Node {self.id} received append entries term:{request.term} leaderId:{request.leaderId} "
                        f"prevLogIndex:{request.prevLogIndex} prevLogTerm:{request.prevLogTerm} entries:{request.entries} leaderCommit:{request.leaderCommit}")
        response = AppendEntriesResponse()
        if request.term < self.currentTerm:
            response.success = False
            return  response

        if len(request.entries) == 0 and request.leaderId != self.id:
            logger.info(f"Node {self.id} may got heartbeat, self term is {self.currentTerm}")
            if request.term>= self.currentTerm:
                response.success = True
                self.currentTerm = request.term
                logger.info(f"Node {self.id} becomes follower, self term is {self.currentTerm}")
                await self.set_role(Role.FOLLOWER)
                return  response
            else:
                response.success = False
                logger.info(f"Node {self.id} coming term is {request.term} less than {self.currentTerm}")
                response.term = self.currentTerm
                return response

        self.currentTerm = request.term
        self.votedFor = None

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
    
    async def getAll(self):
        return {"currentTerm": self.currentTerm, "votedReceived": self.votedReceived, "votedFor": self.votedFor, "commitIndex": self.commitIndex, "log": self.log, "role": self.role}

    async def serve(self, info):
        self.id = info.id
        logger.info(f"Node {self.id} started {info.port}")
        server = grpc.aio.server()
        raft_pb2_grpc.add_RaftServicer_to_server(self, server)
        server.add_insecure_port("localhost:"+(str(info.port)))

        await server.start()
        logger.info("Node started {}".format(info.peers))

        for peer in info.peers:
            peer = grpc.aio.insecure_channel(peer)
            self.channels.append(peer)
            stub = raft_pb2_grpc.RaftStub(peer)
            self.peers.append(stub)
            logger.info(f"Node {self.id} connected to peer {peer} and stub is {stub}")

        await server.wait_for_termination()

    # 用协程方式运行 FastAPI
    async def serve_http(self, info):
        config = uvicorn.Config(self.app, host="0.0.0.0", port=info.port+3000, loop="asyncio")
        server = uvicorn.Server(config)
        await server.serve()


logger = logging.getLogger(__name__)
class ParseInfo:
    def __init__(self):
        self.id = None
        self.peers = None
        self.port = None
# 运行 gRPC + HTTP（FastAPI）
async def main(info):
    node = Node()
    await asyncio.gather(  # 并行运行 gRPC 和 HTTP
        node.serve(info),
        node.serve_http(info)
    )
if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', level=logging.DEBUG)
    parser = argparse.ArgumentParser(description="Raft Node")
    parser.add_argument("--id", type=int, help="Node ID", required=True)
    parser.add_argument("--peers", type=lambda s: s.split(','), help="Peer addrs", required=True)
    parser.add_argument("--port", type=int, help="Port", default=5003)
    info = ParseInfo()
    info.id = parser.parse_args().id
    info.peers = parser.parse_args().peers
    info.port = parser.parse_args().port
    asyncio.run(main(info))