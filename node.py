from enum import Enum

class Role(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

class Node:
    def __init__(self):
        self.role = Role.FOLLOWER
        self.currentTerm = 0
        self.votedReceived = 0
        self.votedFor = None

        self.commitIndex = 0
        self.log = []
        self.peers = []

    def set_role(self, role):
        if isinstance(role, Role):
            self.role = role
        else:
            raise ValueError("Invalid role")

    def requestVote(self, request, response):
        if request.term < self.currentTerm:
            response.voteGranted = False
            return

        if request.term > self.currentTerm:
            self.currentTerm = request.term
            self.votedFor = None

        if self.votedFor is None or self.votedFor == request.candidateId:
            self.votedFor = request.candidateId
            response.voteGranted = True
        else:
            response.voteGranted = False

    def leaderElection(self):
        self.set_role(Role.CANDIDATE)
        self.currentTerm += 1
        self.votedFor = None
        for peer in self.peers:
            request = RequestVoteRequest()
            request.candidateId = self.id
            request.term = self.currentTerm
            request.lastLogIndex = len(self.log) - 1
            request.lastLogTerm = self.log[-1].term
            response = RequestVoteResponse()
            peer.requestVote(request, response)
            if response.voteGranted:
                self.votedReceived += 1
                if self.votedReceived > len(self.peers) / 2:
                    self.set_role(Role.LEADER)
                    return
                
    def heartbeat(self):
        for peer in self.peers:
            request = AppendEntriesRequest()
            response = AppendEntriesResponse()
            peer.appendEntries(request, response)


    def appendEntries(self, request, response):
        if request.term < self.currentTerm:
            response.success = False
            return

        self.currentTerm = request.term
        self.votedFor = None

        if len(self.log) < request.prevLogIndex or self.log[request.prevLogIndex].term != request.prevLogTerm:
            response.success = False
            return

        self.log = self.log[:request.prevLogIndex + 1]
        self.log.extend(request.entries)

        if request.leaderCommit > self.commitIndex:
            self.commitIndex = min(request.leaderCommit, len(self.log) - 1)

        response.success = True


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
            response = AppendEntriesResponse()
            peer.appendEntries(request, response)
            if response.success:
                appendCnt += 1
        if appendCnt > len(self.peers) / 2:
            self.commitIndex += 1
            self.log.append(entry)
        else:
            return False
        return True
    
    def stateMachineLoop(self):
        while True:
            if self.role == Role.LEADER:
                self.heartbeat()
            elif self.role == Role.CANDIDATE:
                self.leaderElection()
            else:
                pass