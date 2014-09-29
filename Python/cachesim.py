# Karol Pogonowski - Parallel Architectures Assigment
# This code is a cache simulator. It reads an example memory trace specified
# a priori and then simulates the workings on a processors cache.
__author__ = 'Karol Pogonowski'
from string import atoi
import random
import sys

num_addr = 2048

CACHE_HIT = 2
BUS = 20
MAIN_MEMORY = 200
READ_BYPASS = 1


class State():
    M = 0
    S = 1
    I = 2


class CacheSim():
    # This constructor sets up a lot of important things
    def __init__(self, source):
        file = open(source, 'r')
        self.fileData = file.readlines()
        file.close()
        # Direct mapped writeThrough and writeAllocate cache with 4 cpu and LRU
        self.numCPU = 4
        self.cacheSize = 128
        self.blockSize = 4
        self.associativity = 1
        self.writeBack = False
        self.writeAllocate = True
        self.replacement = 1
        self.verbose = False
        self.fullVerbose = False
        self.bufferLimit = 32
        self.prefetchOffset = 0
        self.retireN = 8
        self.addrAccess = [[0 for j in range(0, self.numCPU)] for i in
                            range(0, num_addr)]
        self.privAccess = 0
        self.sharedRead = 0
        self.sharedReadWrite = 0

        # Read arguments
        for i in range(0, len(sys.argv)):
            arg = sys.argv[i]
            arg.split(' ', 2)
            if arg[0] == "cache":
                self.cacheSize = atoi(arg[1])
            if arg[0] == "block":
                self.blockSize = atoi(arg[1])
            if arg[0] == "assoc":
                self.associativity = atoi(arg[1])
            if arg[0] == "wb":
                self.writeBack = True
            if arg[0] == "wa":
                self.writeAllocate = True
            if arg[0] == "buffer":
                self.bufferLimit = atoi(arg[1])
            if arg[0] == "prefetch":
                self.prefetchOffset = atoi(arg[1])
            if arg[0] == "replacement":
                self.replacement = atoi(arg[1])
            if arg[0] == "cpu":
                self.numCPU = atoi(arg[1])
            if arg[0] == "retire":
                self.retireN = atoi(arg[1])
            if arg[0] == "v":
                self.verbose = atoi(arg[1])
            if arg[0] == "fv":
                self.fullVerbose = atoi(arg[1])

        self.statReadHit = [0 for i in range(0, self.numCPU)]
        self.statReadMiss = [0 for i in range(0, self.numCPU)]
        self.statWriteHit = [0 for i in range(0, self.numCPU)]
        self.statWriteMiss = [0 for i in range(0, self.numCPU)]
        self.invalidations = [0 for i in range(0, self.numCPU)]
        self.statRAM = [0 for i in range(0, self.numCPU)]
        self.coherenceMiss = [0 for i in range(0, self.numCPU)]
        self.counter = [0 for i in range(0, self.numCPU)]
        self.bufferCounter = [0 for i in range(0, self.numCPU)]
        self.txtBuffer = ""
        self.time = 0
        self.writeBuffer = [[] for j in range(0, self.numCPU)]
        self.cache = [[[(-1, State.I) for x in range(0, self.associativity)]
                        for i in range(0, self.cacheSize/self.associativity)]
                        for j in range(0, self.numCPU)]
        self.cacheTimer = [[[-1 for x in range(0, self.associativity)] 
                        for i in range(0, self.cacheSize/self.associativity)]
                        for j in range(0, self.numCPU)]

    # Decodes the given address and returns a computed tag, index and byte 
    # offset within the block
    def decode(self, address):
        offset = address / self.blockSize
        index = offset % (self.cacheSize / self.associativity)
        tag = offset / (self.cacheSize / self.associativity)
        return tag, index, address % self.blockSize

    # Block replacement function. Takes an address to write and calculates
    # where we can write it. Also updates cacheTimers
    def writeCache(self, address, proc, state=State.S):
        if self.fullVerbose:
            print "Cache Write Access"
        (tag, index, offset) = self.decode(address)
        idx = -1
        # See if there is any free bit we could use
        for (temp, flag) in self.cache[proc][index]:
            if flag == State.I and temp == -1:
                idx = self.cache[proc][index].index((temp, flag))
                break
        # No luck, fire up replacement
        if idx == -1:
            if self.replacement == 1:
                # LRU
                idx = self.cacheTimer[proc][index].index(max(
                    self.cacheTimer[proc][index]))
            elif self.replacement == 2:
                # MRU
                idx = self.cacheTimer[proc][index].index(min(
                    self.cacheTimer[proc][index]))
            else:
                # Random
                idx = random.randint(0, self.associativity-1)

        # Address to evict
        (evictTag, evictFlag) = self.cache[proc][index][idx]
        if self.fullVerbose:
            if evictFlag == State.M:
                temp = "M"
            elif evictFlag == State.S:
                temp = "S"
            elif evictFlag == State.I:
                temp = "I"
            print "Evicting block %d from set %d with tag %d and flag %s" \
                    % (idx, index, evictTag, temp)

        self.cache[proc][index][idx] = (tag, state)
        self.cacheTimer[proc][index][idx] = 0
        # Increment other timers
        self.incrementTimers(index, idx, proc)

    # Increments all timers besides sent one. Used for LRU and MRU.
    def incrementTimers(self, index, idx, proc):
        for i in range(0, len(self.cacheTimer[proc])):
            for j in range(0, len(self.cacheTimer[proc][i])):
                if (i == index and j != idx) or (i != index):
                    self.cacheTimer[proc][i][j] += 1

    # Returns true if the given address is in local cache
    def isCached(self, address, proc):
        (tag, index, offset) = self.decode(address)
        # Update processor self.counter
        self.counter[proc] += CACHE_HIT
        if self.fullVerbose:
            print "Updating counter - cache access"
        self.txtBuffer += "looked for tag %d in set %d and offset %d on \
                           processor %d " % (tag, index, offset, proc)    
        for (data, flag) in self.cache[proc][index]:
            if data == tag:
                if flag != State.I:
                    # Increment other cache lines' age
                    idx = self.cache[proc][index].index((data, flag))
                    self.incrementTimers(index, idx, proc)
                    self.txtBuffer += "and was a hit in block %d" % idx
                    if self.fullVerbose:
                        print self.txtBuffer
                    self.txtBuffer = ""
                    return True
                else:
                    self.coherenceMiss[proc] += 1

        self.txtBuffer += "and was a miss"
        if self.fullVerbose:
            print self.txtBuffer
        self.txtBuffer = ""
        return False

    # An interface to write to RAM. If write buffer is set, writes there first.
    # Also handles write buffer overgrowth (writes old data to RAM).
    def writeRAM(self, address, proc, directMem=False):
        if directMem:
            (tag, index) = address
        else:
            (tag, index, _) = self.decode(address)
        # Check for write buffer
        if self.bufferLimit > 0:  
            # Write buffer is full, wait for our turn
            if len(self.writeBuffer[proc]) == self.bufferLimit:
                # Wait for our turn
                if (self.bufferCounter[proc] > self.counter[proc]):
                    self.counter[proc] = self.bufferCounter[proc] + MAIN_MEMORY
                    if self.fullVerbose:
                        print "Write extra buffer stall"
                else:
                    self.counter[proc] += MAIN_MEMORY
                    if self.fullVerbose:
                        print "Write buffer stall"
                (_, _) = self.writeBuffer[proc].pop(0)
                self.statRAM[proc] += 1
                if self.fullVerbose:
                    print "Write RAM Access"
            self.writeBuffer[proc].append((tag, index))
            if self.fullVerbose:
                print "WriteBuffer write Access"
        else:
            self.statRAM[proc] += 1
            if self.fullVerbose:
                print "Write RAM Access"
            # Update processor self.counter
            self.counter[proc] += MAIN_MEMORY
            if self.fullVerbose:
                print "Updating counter - main memory access"

    # Loads address from RAM. Looks in write buffer first.
    def loadRAM(self, address, proc):
        (tag, index, offset) = self.decode(address)
        self.statRAM[proc] += 1
        if self.fullVerbose:
            print "Load RAM Access"
        # Update processor self.counter
        self.counter[proc] += MAIN_MEMORY
        if self.fullVerbose:
            print "Updating counter - main memory access"

    # Prefetches memory near this address using Stride Prefetching. The address
    # offset of the prefetched memory is set as argument. In most cases this
    # should be equal to 1 (next blocks prefetching).
    def prefetch(self, address, proc):
        if self.prefetchOffset > 0:
            pre = address + self.prefetchOffset
            temp = "Prefetching address %d: " % pre
            # See if our data is in cache already
            if not self.isCached(pre, proc):
                self.loadRAM(pre, proc)
                self.writeCache(pre, proc)
                temp += "success"
            else:
                temp += "already cached"
            if self.fullVerbose:
                print temp

    # See if we can retire something from the write-buffer
    def bufferRetire(self, proc):
        # See if we should retire
        if len(self.writeBuffer[proc]) > self.retireN:
            # Wait for our turn
            if (self.bufferCounter[proc] > self.counter[proc]):
                if self.fullVerbose:
                    print "Waiting for write to finish"
                return
            (_, _) = self.writeBuffer[proc].pop(0)
            self.statRAM[proc] += 1
            if self.fullVerbose:
                print "Write-buffer RAM Access"
                # Update buffer wait self.counter
            self.bufferCounter[proc] = self.counter[proc] + MAIN_MEMORY

    # Handle a load call from the trace file. See if data is cached, updated
    # stats. Otherwise, load RAM and write to cache.
    def readHandler(self, address, proc):
        # Data request
        (tag, index, _) = self.decode(address)
        # Update statistics
        self.addrAccess[address][proc] = 1
        self.bufferRetire(proc)
        # Read bypass
        # See if we got a hit in the write buffer
        if self.bufferLimit > 0:
            try:
                self.writeBuffer[proc].index((tag, index))
                if self.fullVerbose:
                    self.txtBuffer += " and was hit in write-buffer"
                    print self.txtBuffer
                    self.txtBuffer = ""
                    print "WriteBuffer Read Access"

                # Update processor self.counter
                self.counter[proc] += READ_BYPASS
                if self.fullVerbose:
                    print "Updating counter - read bypass"
                return
            except ValueError:
                pass

        # See if our data is in the cache
        data = self.isCached(address, proc)
        if data:
            # Cached. Load to processor. Update statistics.
            shared = 0
            modified = 0
            for i in range(0, self.numCPU):
                try:
                    idx = self.cache[i][index].index((tag, State.S))
                    # Found shared in other
                    shared += 1
                except ValueError:
                    pass
                try:
                    idx = self.cache[i][index].index((tag, State.M))
                    # Found modified in other
                    modified += 1
                except ValueError:
                    pass
            if (shared == 1 and modified == 0) or \
                (shared == 0 and modified == 1):
                self.privAccess += 1
            elif shared > 1 and modified == 0:
                self.sharedRead += 1
            else:
                self.sharedReadWrite += 1
            self.statReadHit[proc] += 1
            self.prefetch(address, proc)
        else:
            # Not cached. Fetch the data from RAM and write to cache
            # See if the line is modified somewhere else
            for i in range(0, self.numCPU):
                if i != proc:
                    try:
                        idx = self.cache[i][index].index((tag, State.M))
                        if self.fullVerbose:
                            print "Snooping: found modified tag in processor's\
                                    %d cache, committing and setting shared" \
                                    % i
                        if not self.writeBack:
                            self.writeRAM(address, proc)
                        # Write to cache and set to be shared
                        self.cache[i][index][idx] = (tag, State.S)
                    except ValueError:
                        pass
            # Update processor self.counter
            self.counter[proc] += BUS
            if self.fullVerbose:
                print "Updating counter - bus transaction"
            self.statReadMiss[proc] += 1
            if not self.writeBack:
                self.loadRAM(address, proc)
            self.writeCache(address, proc)
            self.prefetch(address, proc)

    # Handle a store call from the trace file. See if data is cached, update 
    # stats. See if write-back is needed. Otherwise, load RAM and write to 
    # cache if write-allocate, or write to RAM if write-no-allocate.
    def writeHandler(self, address, proc):
        # Data request
        # See if our data is in the cache
        data = self.isCached(address, proc)
        (tag, index, _) = self.decode(address)
        # Update statistics
        self.addrAccess[address][proc] = 1
        self.bufferRetire(proc)

        if data:
            # Cached. Write to cache
            # We write data to cache here. This is not a real operation as no 
            # data is transferred and the block tag is already in the cache.
            # Check if we need to write lower level memory too.
            if self.fullVerbose:
                print "Writing to data in cache"
            # Update statistics
            shared = 0
            modified = 0
            for i in range(0, self.numCPU):
                try:
                    idx = self.cache[i][index].index((tag, State.S))
                    # Found shared in other
                    shared += 1
                except ValueError:
                    pass
                try:
                    idx = self.cache[i][index].index((tag, State.M))
                    # Found modified in other
                    modified += 1
                except ValueError:
                    pass
                    
            if (shared == 1 and modified == 0) or \
                (shared == 0 and modified == 1):
                self.privAccess += 1
            else:
                self.sharedReadWrite += 1
                
            # Need to invalidate the data for other caches
            # Actual snooping takes place here
            # Either modified or shared in our cache
            try:
                our_idx = self.cache[proc][index].index((tag, State.M))
                if self.fullVerbose:
                    print "Cache modified"
                self.statWriteHit[proc] += 1
            except ValueError:
                pass
            try:
                our_idx = self.cache[proc][index].index((tag, State.S))
                if self.fullVerbose:
                    print "Cache shared"
                # Write to shared is a miss
                self.statWriteMiss[proc] += 1
            except ValueError:
                pass

            (_, flag) = self.cache[proc][index][our_idx]
            if flag == State.S:
                for i in range(0, self.numCPU):
                    if i != proc:
                        try:        
                            # Found it modified in another's cache, need to 
                            # get him to commit
                            idx = self.cache[i][index].index((tag, State.M))
                            if self.fullVerbose:
                                print "Snooping: found modified tag in \
                                        processor's %d cache, committing and \
                                        invalidating" % i
                            if not self.writeBack:
                                self.writeRAM(address, proc)
                            # Invalidate this cache for now
                            self.invalidations[proc] += 1
                            self.cache[i][index][idx] = (tag, State.I)
                        except ValueError:
                            pass
                        try:                    
                            # Found it shared in another's cache, invalidate
                            idx = self.cache[i][index].index((tag, State.S))
                            if self.fullVerbose:
                                print "Snooping: found shared tag in \
                                        processor's %d cache, invalidating" % i    
                            self.invalidations[proc] += 1
                            self.cache[i][index][idx] = (tag, State.I)
                        except ValueError:
                            pass                                
            # Update processor self.counter
            self.counter[proc] += BUS
            if self.fullVerbose:
                print "Updating counter - bus transaction"                
            # Set our state to modified
            self.cache[proc][index][our_idx] = (tag, State.M)
            if not self.writeBack:
                self.writeRAM(address, proc)
            self.prefetch(address, proc)
        else:
            # Not cached or invalidated
            # Check other caches first
            for i in range(0, self.numCPU):
                if i != proc:
                    try:        
                        # Found it modified in another's cache, need to get him
                        # to commit
                        idx = self.cache[i][index].index((tag, State.M))
                        if self.fullVerbose:
                            print "Snooping: found modified tag in processor's \
                                    %d cache, committing and invalidating" % i
                        if not self.writeBack:
                            self.writeRAM(address, proc)
                         # Write to cache and invalidate
                        self.invalidations[proc] += 1
                        self.cache[i][index][idx] = (tag, State.I)
                    except ValueError:
                        pass
                    try:                    
                        # Found it shared in another's cache, invalidate
                        idx = self.cache[i][index].index((tag, State.S))
                        if self.fullVerbose:
                            print "Snooping: found shared tag in processor's %d \
                                    cache, invalidating" % i
                        self.invalidations[proc] += 1
                        self.cache[i][index][idx] = (tag, State.I)
                    except ValueError:
                        pass
                
            # Update processor self.counter
            self.counter[proc] += BUS
            if self.fullVerbose:
                print "Updating counter - bus transaction"
            # Check if we need to add the missed data to the cache.
            if self.writeAllocate:
                self.writeCache(address, proc, State.M)
            else:
                self.writeRAM(address, proc)
            self.statWriteMiss[proc] += 1
            self.prefetch(address, proc)

    # Main method. Reads data from the trace file and calls appropriate 
    # handlers. Implements all rudimentary trace file options, as well as full
    # verbose option, which gives even more data than verbose alone.
    def run(self):
        for i in range(0, len(self.fileData)):
            a = self.fileData[i]
            # Data access
            if a[0] == '#':
                continue
            elif a[0] == 'P':
                proc_num = atoi(a[1])
                if a[3] == 'R':
                    # Load instruction
                    addr = a.split(' ', 3)[2]
                    self.txtBuffer += "\nRead from address %d on processor %d "\
                        % (atoi(addr), proc_num)
                    self.readHandler(atoi(addr), proc_num)
                elif a[3] == 'W':
                    # Store instruction
                    addr = a.split(' ', 3)[2]
                    self.txtBuffer += "\nWrite to address %d on processor %d "\
                        % (atoi(addr), proc_num)
                    self.writeHandler(atoi(addr), proc_num)
            elif a[0] == 'D':
                # Dump cache
                print "\n\t\t\t\t\t\tDumping cache"
                for proc in range(0, self.numCPU):
                    print "\nProcessor %d\n" % proc
                    for i in range(0, self.cacheSize/self.associativity):
                        if len(self.cache[proc][i]) > 0:
                                print "Set %d" % i
                                print"Flag\t\tBlock\t\tTag\t\tAge"
                                print"-------------------------------------"
                        for j in range(0, len(self.cache[proc][i])):
                            (va, ta) = self.cache[proc][i][j]
                            temp = ""
                            if ta == State.M:
                                temp = 'M'
                            elif ta == State.S:
                                temp = 'S'
                            elif ta == State.I:
                                temp = 'I'
                            print temp + "\t\t" + str(j) + "\t\t" + str(va) + \
                                "\t\t" + str(self.cacheTimer[proc][i][j])
                            print ""
                    if self.bufferLimit > 0:
                        print "\t\t\t\t\t\tWrite buffer"
                        print"Tag\tIndex"
                        print"------------"
                        for i in range(0, len(self.writeBuffer[proc])):
                            (bTag, bIndex) = self.writeBuffer[proc][i]
                            print str(bTag) + "\t" + str(bIndex)
                            print "\n"                

        print 'Finished. Displaying statistics \n'
        for j in range(0, self.numCPU):
            print "Proc %d\n" % j
            if (self.statReadMiss[j] + self.statReadHit[j]) > 0:
                print 'Load: %f%%' % ((100.0 * self.statReadHit[j] / 
                    (self.statReadMiss[j] + self.statReadHit[j])))
            if (self.statWriteMiss[j] + self.statWriteHit[j]) > 0:
                print 'Store: %f%%' % ((100.0 * self.statWriteHit[j] / 
                    (self.statWriteMiss[j] + self.statWriteHit[j])))
            if (self.statReadMiss[j] + self.statReadHit[j]) > 0 or \
                (self.statWriteMiss[j] + self.statWriteHit[j]) > 0:
                print 'Hit rate: %f%%' % (100 * (self.statReadHit[j] 
                    + self.statWriteHit[j])/(1.0 * self.statReadHit[j] 
                    + self.statWriteHit[j] + self.statReadMiss[j] 
                    + self.statWriteMiss[j]))
            if (self.statReadMiss[j] + self.statWriteMiss[j]) > 0:
                print "Percentage of coherence misses: %f%%" % (100 
                    * self.coherenceMiss[j]/(1.0 * self.statReadMiss[j] 
                    + self.statWriteMiss[j]))
            print 'Instruction counter: %d' % self.counter[j]
            print 'RAM accesses: %d' % self.statRAM[j]
            print "Number of invalidations: %d" % self.invalidations[j]
            print '\n'

        acc_sum = 1.0 * (self.privAccess + self.sharedRead 
            + self.sharedReadWrite)
        print "Private cache-line hits: %f%%" % (self.privAccess / acc_sum 
            * 100)
        print "Shared read-only cache-line hits: %f%%" % (self.sharedRead 
            / acc_sum * 100)
        print "Shared read-write cache-line hits: %f%%" % (self.sharedReadWrite
            / acc_sum * 100)
        num_1proc = 0
        num_2proc = 0
        num_procs = 0

        for i in range(0, num_addr):
            acc_sum = 0
            for j in range(0, self.numCPU):
                acc_sum += self.addrAccess[i][j]
            if acc_sum == 1:
                num_1proc += 1
            elif acc_sum == 2:
                num_2proc += 1
            elif acc_sum > 2:
                num_procs += 1
        acc_sum = 1.0 * (num_1proc + num_2proc + num_procs)

        print "1 processor access: %f%%" % (num_1proc / acc_sum * 100)
        print "2 processors access: %f%%" % (num_2proc / acc_sum * 100)
        print ">2 processors access: %f%%" % (num_procs / acc_sum * 100)

args = "trace2.out"
print "Opening file " + args
a = CacheSim(args)
a.run()
