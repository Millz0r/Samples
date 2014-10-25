#!/usr/bin/env python
# Karol Pogonowski
# This code is a cache simulator. It reads an example memory trace specified
# a priori and then simulates the workings on a processors cache.
from string import atoi
import random
import sys

# Constants
NUM_ADDR = 2048
CACHE_HIT = 2
BUS = 20
MAIN_MEMORY = 200
READ_BYPASS = 1


class State:
    M = 0
    S = 1
    I = 2


class CacheSim:
    def __init__(self, source):
        """Set up parameters and read the input file"""
        file = open(source, 'r')
        self.file_data = file.readlines()
        file.close()
        self.num_cpu = 4
        self.cache_size = 128
        self.block_size = 4
        self.associativity = 1
        self.write_back = False
        self.write_allocate = True
        self.replacement = 1
        self.verbose = False
        self.full_verbose = False
        self.buffer_limit = 32
        self.prefetch_offset = 0
        self.retire_num = 8
        self.addr_access = [[0 for j in range(0, self.num_cpu)] for i in
                            range(0, NUM_ADDR)]
        self.priv_access = 0
        self.shared_read = 0
        self.shared_read_write = 0

        # Read arguments
        for i in range(0, len(sys.argv)):
            arg = sys.argv[i]
            arg.split(' ', 2)
            if arg[0] == "cache":
                self.cache_size = atoi(arg[1])
            if arg[0] == "block":
                self.block_size = atoi(arg[1])
            if arg[0] == "assoc":
                self.associativity = atoi(arg[1])
            if arg[0] == "wb":
                self.write_back = True
            if arg[0] == "wa":
                self.write_allocate = True
            if arg[0] == "buffer":
                self.buffer_limit = atoi(arg[1])
            if arg[0] == "prefetch":
                self.prefetch_offset = atoi(arg[1])
            if arg[0] == "replacement":
                self.replacement = atoi(arg[1])
            if arg[0] == "cpu":
                self.num_cpu = atoi(arg[1])
            if arg[0] == "retire":
                self.retire_num = atoi(arg[1])
            if arg[0] == "v":
                self.verbose = atoi(arg[1])
            if arg[0] == "fv":
                self.full_verbose = atoi(arg[1])

        self.stat_read_hit = [0 for i in range(0, self.num_cpu)]
        self.stat_read_miss = [0 for i in range(0, self.num_cpu)]
        self.stat_write_hit = [0 for i in range(0, self.num_cpu)]
        self.stat_write_miss = [0 for i in range(0, self.num_cpu)]
        self.invalidations = [0 for i in range(0, self.num_cpu)]
        self.stat_ram = [0 for i in range(0, self.num_cpu)]
        self.coherence_miss = [0 for i in range(0, self.num_cpu)]
        self.counter = [0 for i in range(0, self.num_cpu)]
        self.buffer_counter = [0 for i in range(0, self.num_cpu)]
        self.txt_buffer = ""
        self.time = 0
        self.write_buffer = [[] for j in range(0, self.num_cpu)]
        self.cache = [[[
            (-1, State.I) for x in range(0, self.associativity)]
            for i in range(0, self.cache_size/self.associativity)]
            for j in range(0, self.num_cpu)]
        self.cache_timer = [[[
            -1 for x in range(0, self.associativity)]
            for i in range(0, self.cache_size/self.associativity)]
            for j in range(0, self.num_cpu)]

    def decode(self, address):
        """Decodes the given address and returns a computed tag, index and byte
        offset within the block"""
        offset = address / self.block_size
        index = offset % (self.cache_size / self.associativity)
        tag = offset / (self.cache_size / self.associativity)
        return tag, index, address % self.block_size

    def write_cache(self, address, proc, state=State.S):
        """Block replacement function. Takes an address to write and calculates
        where we can write it. Also updates cache_timers"""
        if self.full_verbose:
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
                idx = self.cache_timer[proc][index].index(max(
                    self.cache_timer[proc][index]))
            elif self.replacement == 2:
                # MRU
                idx = self.cache_timer[proc][index].index(min(
                    self.cache_timer[proc][index]))
            else:
                # Random
                idx = random.randint(0, self.associativity-1)
        # Address to evict
        (evictTag, evictFlag) = self.cache[proc][index][idx]
        if self.full_verbose:
            if evictFlag == State.M:
                temp = "M"
            elif evictFlag == State.S:
                temp = "S"
            elif evictFlag == State.I:
                temp = "I"
            print "Evicting block %d from set %d with tag %d and flag %s" \
                  % (idx, index, evictTag, temp)

        self.cache[proc][index][idx] = (tag, state)
        self.cache_timer[proc][index][idx] = 0
        # Increment other timers
        self.increment_timers(index, idx, proc)

    def increment_timers(self, index, idx, proc):
        """Increments all timers besides sent one. Used for LRU and MRU"""
        for i in range(0, len(self.cache_timer[proc])):
            for j in range(0, len(self.cache_timer[proc][i])):
                if (i == index and j != idx) or (i != index):
                    self.cache_timer[proc][i][j] += 1

    def is_cached(self, address, proc):
        """Returns true if the given address is in local cache"""
        (tag, index, offset) = self.decode(address)
        # Update processor self.counter
        self.counter[proc] += CACHE_HIT
        if self.full_verbose:
            print "Updating counter - cache access"
        self.txt_buffer += "looked for tag %d in set %d and offset %d on \
                           processor %d " % (tag, index, offset, proc)
        for (data, flag) in self.cache[proc][index]:
            if data == tag:
                if flag != State.I:
                    # Increment other cache lines' age
                    idx = self.cache[proc][index].index((data, flag))
                    self.increment_timers(index, idx, proc)
                    self.txt_buffer += "and was a hit in block %d" % idx
                    if self.full_verbose:
                        print self.txt_buffer
                    self.txt_buffer = ""
                    return True
                else:
                    self.coherence_miss[proc] += 1
        self.txt_buffer += "and was a miss"
        if self.full_verbose:
            print self.txt_buffer
        self.txt_buffer = ""
        return False

    def write_ram(self, address, proc, directMem=False):
        """An interface to write to RAM. If write buffer is set, writes there
        first. Also handles write buffer overgrowth (writes old data to RAM)"""
        if directMem:
            (tag, index) = address
        else:
            (tag, index, _) = self.decode(address)
        # Check for write buffer
        if self.buffer_limit > 0:
            # Write buffer is full, wait for our turn
            if len(self.write_buffer[proc]) == self.buffer_limit:
                # Wait for our turn
                if (self.buffer_counter[proc] > self.counter[proc]):
                    self.counter[proc] = self.buffer_counter[proc]\
                        + MAIN_MEMORY
                    if self.full_verbose:
                        print "Write extra buffer stall"
                else:
                    self.counter[proc] += MAIN_MEMORY
                    if self.full_verbose:
                        print "Write buffer stall"
                (_, _) = self.write_buffer[proc].pop(0)
                self.stat_ram[proc] += 1
                if self.full_verbose:
                    print "Write RAM Access"
            self.write_buffer[proc].append((tag, index))
            if self.full_verbose:
                print "WriteBuffer write Access"
        else:
            self.stat_ram[proc] += 1
            if self.full_verbose:
                print "Write RAM Access"
            # Update processor self.counter
            self.counter[proc] += MAIN_MEMORY
            if self.full_verbose:
                print "Updating counter - main memory access"

    def load_ram(self, address, proc):
        """Loads address from RAM. Looks in write buffer first."""
        (tag, index, offset) = self.decode(address)
        self.stat_ram[proc] += 1
        if self.full_verbose:
            print "Load RAM Access"
        # Update processor self.counter
        self.counter[proc] += MAIN_MEMORY
        if self.full_verbose:
            print "Updating counter - main memory access"

    def prefetch(self, address, proc):
        """Prefetches memory near this address using Stride Prefetching. The
        address offset of the prefetched memory is set as argument. In most
        cases this should be equal to 1 (next blocks prefetching)"""
        if self.prefetch_offset > 0:
            pre = address + self.prefetch_offset
            temp = "Prefetching address %d: " % pre
            # See if our data is in cache already
            if not self.is_cached(pre, proc):
                self.load_ram(pre, proc)
                self.write_cache(pre, proc)
                temp += "success"
            else:
                temp += "already cached"
            if self.full_verbose:
                print temp

    def buffer_retire(self, proc):
        """See if we can retire something from the write buffer"""
        # See if we should retire
        if len(self.write_buffer[proc]) > self.retire_num:
            # Wait for our turn
            if (self.buffer_counter[proc] > self.counter[proc]):
                if self.full_verbose:
                    print "Waiting for write to finish"
                return
            (_, _) = self.write_buffer[proc].pop(0)
            self.stat_ram[proc] += 1
            if self.full_verbose:
                print "Write-buffer RAM Access"
                # Update buffer wait self.counter
            self.buffer_counter[proc] = self.counter[proc] + MAIN_MEMORY

    def handle_read(self, address, proc):
        """Handle a load call from the trace file. See if data is cached,
        update stats. Otherwise, load RAM and write to cache."""
        # Data request
        (tag, index, _) = self.decode(address)
        # Update statistics
        self.addr_access[address][proc] = 1
        self.buffer_retire(proc)
        # Read bypass
        # See if we got a hit in the write buffer
        if self.buffer_limit > 0:
            try:
                self.write_buffer[proc].index((tag, index))
                if self.full_verbose:
                    self.txt_buffer += " and was hit in write-buffer"
                    print self.txt_buffer
                    self.txt_buffer = ""
                    print "WriteBuffer Read Access"
                # Update processor self.counter
                self.counter[proc] += READ_BYPASS
                if self.full_verbose:
                    print "Updating counter - read bypass"
                return
            except ValueError:
                pass
        # See if our data is in the cache
        data = self.is_cached(address, proc)
        if data:
            # Cached. Load to processor. Update statistics.
            shared = 0
            modified = 0
            for i in range(0, self.num_cpu):
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
                self.priv_access += 1
            elif shared > 1 and modified == 0:
                self.shared_read += 1
            else:
                self.shared_read_write += 1
            self.stat_read_hit[proc] += 1
            self.prefetch(address, proc)
        else:
            # Not cached. Fetch the data from RAM and write to cache
            # See if the line is modified somewhere else
            for i in range(0, self.num_cpu):
                if i != proc:
                    try:
                        idx = self.cache[i][index].index((tag, State.M))
                        if self.full_verbose:
                            print "Snooping: found modified tag in processor's\
                                    %d cache, committing and setting shared" \
                                    % i
                        if not self.write_back:
                            self.write_ram(address, proc)
                        # Write to cache and set to be shared
                        self.cache[i][index][idx] = (tag, State.S)
                    except ValueError:
                        pass
            # Update processor self.counter
            self.counter[proc] += BUS
            if self.full_verbose:
                print "Updating counter - bus transaction"
            self.stat_read_miss[proc] += 1
            if not self.write_back:
                self.load_ram(address, proc)
            self.write_cache(address, proc)
            self.prefetch(address, proc)

    def handle_write(self, address, proc):
        """Handle a store call from the trace file. See if data is cached,
        update stats. See if write-back is needed. Otherwise, load RAM and
        write to cache if write-allocate, or write to RAM if
        write-no-allocate"""
        # Data request
        # See if our data is in the cache
        data = self.is_cached(address, proc)
        (tag, index, _) = self.decode(address)
        # Update statistics
        self.addr_access[address][proc] = 1
        self.buffer_retire(proc)
        if data:
            # Cached. Write to cache
            # We write data to cache here. This is not a real operation as no
            # data is transferred and the block tag is already in the cache.
            # Check if we need to write lower level memory too.
            if self.full_verbose:
                print "Writing to data in cache"
            # Update statistics
            shared = 0
            modified = 0
            for i in range(0, self.num_cpu):
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
                self.priv_access += 1
            else:
                self.shared_read_write += 1
            # Need to invalidate the data for other caches
            # Actual snooping takes place here
            # Either modified or shared in our cache
            try:
                our_idx = self.cache[proc][index].index((tag, State.M))
                if self.full_verbose:
                    print "Cache modified"
                self.stat_write_hit[proc] += 1
            except ValueError:
                pass
            try:
                our_idx = self.cache[proc][index].index((tag, State.S))
                if self.full_verbose:
                    print "Cache shared"
                # Write to shared is a miss
                self.stat_write_miss[proc] += 1
            except ValueError:
                pass
            (_, flag) = self.cache[proc][index][our_idx]
            if flag == State.S:
                for i in range(0, self.num_cpu):
                    if i != proc:
                        try:
                            # Found it modified in another's cache, need to
                            # get him to commit
                            idx = self.cache[i][index].index((tag, State.M))
                            if self.full_verbose:
                                print "Snooping: found modified tag in \
                                        processor's %d cache, committing and \
                                        invalidating" % i
                            if not self.write_back:
                                self.write_ram(address, proc)
                            # Invalidate this cache for now
                            self.invalidations[proc] += 1
                            self.cache[i][index][idx] = (tag, State.I)
                        except ValueError:
                            pass
                        try:
                            # Found it shared in another's cache, invalidate
                            idx = self.cache[i][index].index((tag, State.S))
                            if self.full_verbose:
                                print "Snooping: found shared tag in \
                                        processor's %d cache, invalidating" % i
                            self.invalidations[proc] += 1
                            self.cache[i][index][idx] = (tag, State.I)
                        except ValueError:
                            pass
            # Update processor self.counter
            self.counter[proc] += BUS
            if self.full_verbose:
                print "Updating counter - bus transaction"
            # Set our state to modified
            self.cache[proc][index][our_idx] = (tag, State.M)
            if not self.write_back:
                self.write_ram(address, proc)
            self.prefetch(address, proc)
        else:
            # Not cached or invalidated
            # Check other caches first
            for i in range(0, self.num_cpu):
                if i != proc:
                    try:
                        # Found it modified in another's cache, need to get him
                        # to commit
                        idx = self.cache[i][index].index((tag, State.M))
                        if self.full_verbose:
                            print "Snooping: found modified tag in processor's \
                                    %d cache, committing and invalidating" % i
                        if not self.write_back:
                            self.write_ram(address, proc)
                        # Write to cache and invalidate
                        self.invalidations[proc] += 1
                        self.cache[i][index][idx] = (tag, State.I)
                    except ValueError:
                        pass
                    try:
                        # Found it shared in another's cache, invalidate
                        idx = self.cache[i][index].index((tag, State.S))
                        if self.full_verbose:
                            print "Snooping: found shared tag in processor's %d \
                                    cache, invalidating" % i
                        self.invalidations[proc] += 1
                        self.cache[i][index][idx] = (tag, State.I)
                    except ValueError:
                        pass
            # Update processor self.counter
            self.counter[proc] += BUS
            if self.full_verbose:
                print "Updating counter - bus transaction"
            # Check if we need to add the missed data to the cache.
            if self.write_allocate:
                self.write_cache(address, proc, State.M)
            else:
                self.write_ram(address, proc)
            self.stat_write_miss[proc] += 1
            self.prefetch(address, proc)

    def run(self):
        """Main method. Reads data from the trace file and calls appropriate
        handlers. Implements all rudimentary trace file options, as well as
        full verbose option, which gives even more data than verbose alone"""
        for i in range(0, len(self.file_data)):
            a = self.file_data[i]
            # Data access
            if a[0] == '#':
                continue
            elif a[0] == 'P':
                proc_num = atoi(a[1])
                if a[3] == 'R':
                    # Load instruction
                    addr = a.split(' ', 3)[2]
                    self.txt_buffer += "\nRead from address %d on processor %d "\
                        % (atoi(addr), proc_num)
                    self.handle_read(atoi(addr), proc_num)
                elif a[3] == 'W':
                    # Store instruction
                    addr = a.split(' ', 3)[2]
                    self.txt_buffer += "\nWrite to address %d on processor %d "\
                        % (atoi(addr), proc_num)
                    self.handle_write(atoi(addr), proc_num)
            elif a[0] == 'D':
                # Dump cache
                print "\n\t\t\t\t\t\tDumping cache"
                for proc in range(0, self.num_cpu):
                    print "\nProcessor %d\n" % proc
                    for i in range(0, self.cache_size/self.associativity):
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
                                "\t\t" + str(self.cache_timer[proc][i][j])
                            print ""
                    if self.buffer_limit > 0:
                        print "\t\t\t\t\t\tWrite buffer"
                        print"Tag\tIndex"
                        print"------------"
                        for i in range(0, len(self.write_buffer[proc])):
                            (bTag, bIndex) = self.write_buffer[proc][i]
                            print str(bTag) + "\t" + str(bIndex)
                            print "\n"

        print 'Finished. Displaying statistics \n'
        for j in range(0, self.num_cpu):
            print "Proc %d\n" % j
            if (self.stat_read_miss[j] + self.stat_read_hit[j]) > 0:
                print 'Load: %f%%' % ((
                    100.0 * self.stat_read_hit[j] /
                    (self.stat_read_miss[j] + self.stat_read_hit[j])))
            if (self.stat_write_miss[j] + self.stat_write_hit[j]) > 0:
                print 'Store: %f%%' % ((
                    100.0 * self.stat_write_hit[j] /
                    (self.stat_write_miss[j] + self.stat_write_hit[j])))
            if (self.stat_read_miss[j] + self.stat_read_hit[j]) > 0 or \
               (self.stat_write_miss[j] + self.stat_write_hit[j]) > 0:
                print 'Hit rate: %f%%' % (
                    (self.stat_read_hit[j] + self.stat_write_hit[j]) /
                    (1.0 * self.stat_read_hit[j] + self.stat_write_hit[j]
                     + self.stat_read_miss[j] + self.stat_write_miss[j]) * 100)
            if (self.stat_read_miss[j] + self.stat_write_miss[j]) > 0:
                print "Percentage of coherence misses: %f%%" % (
                    100 * self.coherence_miss[j] /
                    (1.0 * self.stat_read_miss[j] + self.stat_write_miss[j]))
            print 'Instruction counter: %d' % self.counter[j]
            print 'RAM accesses: %d' % self.stat_ram[j]
            print "Number of invalidations: %d" % self.invalidations[j]
            print '\n'

        acc_sum = 1.0 * (self.priv_access + self.shared_read
                         + self.shared_read_write)
        print "Private cache-line hits: %f%%" %\
            (self.priv_access / acc_sum * 100)
        print "Shared read-only cache-line hits: %f%%" %\
            (self.shared_read / acc_sum * 100)
        print "Shared read-write cache-line hits: %f%%" %\
              (self.shared_read_write / acc_sum * 100)
        num_1proc = 0
        num_2proc = 0
        num_procs = 0
        for i in range(0, NUM_ADDR):
            acc_sum = 0
            for j in range(0, self.num_cpu):
                acc_sum += self.addr_access[i][j]
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


# Run the simulation
args = "trace2.out"
print "Opening file " + args
sim = CacheSim(args)
sim.run()
