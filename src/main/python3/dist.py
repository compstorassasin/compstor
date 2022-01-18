from absl import app
from absl import flags
from absl import logging

from collections import deque
from heapq import heappop, heappush

import numpy
import pdb

DIST = '''
             0  263208960  289013759   25804800
'''
#            0  263208960  263454719     245760
#    125829120  263520256  263782399     262144
#    260046848  263782400  264011775     229376
#    377487360  264765440  265027583     262144
#    511705088  265027584  265273343     245760
#    637534208  265551872  265814015     262144
#    771751936  265814016  266076159     262144
#    905969664  266076160  266338303     262144
#   1040187392  266338304  266600447     262144
#   1174405120  266600448  266862591     262144
#   1308622848  266862592  267124735     262144
#   1442840576  267124736  267386879     262144
#   1577058304  267386880  267649023     262144
#   1711276032  267649024  267911167     262144
#   1845493760  267911168  268075007     163840
#   1929379840  268845056  269107199     262144
#   2063597568  269107200  269336575     229376
#   2181038080  269778944  270041087     262144
#   2315255808  270041088  270303231     262144
#   2449473536  270303232  270565375     262144
#   2583691264  270565376  270827519     262144
#   2717908992  270827520  271089663     262144
#   2852126720  271089664  271351807     262144
#   2986344448  271351808  271613951     262144
#   3120562176  271613952  271876095     262144
#   3254779904  271876096  272138239     262144
#   3388997632  272138240  272400383     262144
#   3523215360  272400384  272629759     229376
#   3640655872  272891904  273154047     262144
#   3774873600  273154048  273416191     262144
#   3909091328  273416192  273678335     262144
#   4043309056  273678336  273825791     147456
#   4118806528  274333696  274595839     262144
#   4253024256  274595840  274825215     229376
#   4370464768  275283968  275546111     262144
#   4504682496  275546112  275808255     262144
#   4638900224  275808256  276070399     262144
#   4773117952  276070400  276332543     262144
#   4907335680  276332544  276594687     262144
#   5041553408  276594688  276824063     229376
#   5158993920  277086208  277348351     262144
#   5293211648  277348352  277610495     262144
#   5427429376  277610496  277872639     262144
#   5561647104  277872640  278134783     262144
#   5695864832  278134784  278396927     262144
#   5830082560  278396928  278659071     262144
#   5964300288  278659072  278921215     262144
#   6098518016  278921216  279183359     262144
#   6232735744  279183360  279298047     114688
#   6291456000  279805952  280068095     262144
#   6425673728  280068096  280313855     245760
#   6551502848  281591808  281853951     262144
#   6685720576  281853952  282083327     229376
#   6803161088  282574848  282836991     262144
#   6937378816  282836992  283082751     245760
#   7063207936  283590656  283852799     262144
#   7197425664  283852800  284082175     229376
#   7314866176  284327936  284442623     114688
#   7373586432  284459008  284721151     262144
#   7507804160  284721152  284983295     262144
#   7642021888  284983296  285212671     229376
#   7759462400  285474816  285506591      31776

flags.DEFINE_integer("n_core", 8, "Number of cores")
flags.DEFINE_integer("n_channel", 8, "Number of channels")
flags.DEFINE_float("gbps_core", 1, "Core GBPS")
flags.DEFINE_float("gbps_channel", 1, "Channel GBPS")
flags.DEFINE_float("sdev_core", 5e-2, "Core performance variance")
flags.DEFINE_float("sdev_channel", 5e-2, "Channel performance variance")
flags.DEFINE_float("sdev_request", 5e-2, "Request latency dev")
flags.DEFINE_integer("segsize", 512, "Segment size")
flags.DEFINE_integer("pagesize", 4096, "Page size")
flags.DEFINE_integer("job_page", 128, "pages per job")
flags.DEFINE_integer("core_page", 8, "pages holds in each core")
flags.DEFINE_integer("ns_request", 20, "ns to sumbit a request to a channel")

FLAGS = flags.FLAGS


def verify():
    d = numpy.array([int(i) for i in DIST.split()])
    assert (len(d) & 3 == 0)
    d = numpy.reshape(d, (len(d) >> 2, 4))

    sum = 0
    for line in d:
        assert (sum == line[0])
        sum += (line[3] << 9)
        assert (line[3] == line[2] - line[1] + 1)

        page_seg = FLAGS.pagesize // FLAGS.segsize
        job_seg = FLAGS.pagesize // FLAGS.segsize * FLAGS.job_page
        assert ((line[1] & (job_seg - 1)) == 0)
        assert ((line[3] & (job_seg - 1)) == 0)
        line[1] //= page_seg
        line[3] //= page_seg

    return d[:, [1, 3]]


class TWStat:
    def __init__(self):
        self.sum = 0
        self.value = 0
        self.t = 0

    def log(self, value, t):
        assert (t >= self.t), pdb.set_trace()
        self.sum += self.value * (t - self.t)
        self.value = value
        self.t = t

    def average(self, t=None):
        if t is not None:
            self.log(self.value, t)
        return self.sum / self.t


def page_to_channel(page):
    return (page ^ (page >> 4) ^ (page >> 16)) % FLAGS.n_channel


def newtime(t, l, v):
    return t + (1 + numpy.random.normal(scale=v)) * l


class Core:
    def __init__(self):
        self.busy = False
        self.waiting = deque()
        self.toexec = 0
        self.buffer = {}

        self.utilization = TWStat()

    def send_read_requests(self, t, events):
        while (len(self.buffer) < FLAGS.core_page and len(self.waiting) != 0):
            p = self.waiting.popleft()
            self.buffer[p] = False
            nt = newtime(t, FLAGS.ns_request, FLAGS.sdev_request)
            heappush(events, (nt, "R", self, p))

    def check_and_process(self, t, events):
        if self.busy or len(self.buffer) == 0 or not self.buffer[self.toexec]:
            return
        self.busy = True
        self.utilization.log(1, t)

        lpa = self.toexec
        self.toexec += 1

        perf = FLAGS.gbps_core * (1 +
                                  numpy.random.normal(scale=FLAGS.sdev_core))
        nt = t + FLAGS.pagesize / perf
        heappush(events, (nt, "F", self, lpa))


class Channel:
    def __init__(self):
        self.busy = False
        self.queue = deque()

        self.utilization = TWStat()
        self.depth = TWStat()

    def check_and_read(self, t, events):
        if self.busy or len(self.queue) == 0:
            return
        self.busy = True
        self.utilization.log(1, t)
        perf = FLAGS.gbps_channel * (
            1 + numpy.random.normal(scale=FLAGS.sdev_channel))
        nt = t + FLAGS.pagesize / perf
        heappush(events, (nt, "O", self.queue[0][1], self.queue[0][0]))


def dist(argv):
    numpy.random.seed(999)

    d = verify()
    lpas = []
    for l in d:
        lpas.extend(range(l[0], l[0] + l[1], FLAGS.job_page))
    logging.debug("Total jobs: %.2f, %d." %
                  (sum(d[:, 1]) / FLAGS.job_page, len(lpas)))

    events = []
    # (time, code, core, lpa)
    # J, Receiving Job
    # R, Receiving Page read
    # O, Page read finished
    # P, Receiving Page
    # F, Page process finished

    t = 0
    cores = [Core() for _ in range(FLAGS.n_core)]
    coreids = {cores[i]:i for i in range(FLAGS.n_core)}
    channels = [Channel() for _ in range(FLAGS.n_channel)]

    finished = [0 for _ in range(FLAGS.n_core)]

    for i in range(min(len(lpas), FLAGS.n_core)):
        heappush(events, (newtime(t, FLAGS.ns_request,
                                  FLAGS.sdev_request), "J", cores[i], lpas[i]))
    i = min(len(lpas), FLAGS.n_core)

    while ((i < len(lpas)) or (0 != len(events))):
        event = heappop(events)
        logging.debug(event, i, len(lpas))
        t, c, core, lpa = event
        if c == "J":
            assert (len(core.waiting) == 0 and len(core.buffer) == 0)
            core.waiting = deque(range(lpa, lpa + FLAGS.job_page))
            core.toexec = lpa
            core.send_read_requests(t, events)

        elif c == "R":
            channel = channels[page_to_channel(lpa)]
            channel.queue.append((lpa, core))
            channel.depth.log(len(channel.queue), t)
            channel.check_and_read(t, events)

        elif c == "O":
            channel = channels[page_to_channel(lpa)]
            assert channel.queue[0][0] == lpa, pdb.set_trace()
            assert channel.queue[0][1] is core, pdb.set_trace()
            channel.queue.popleft()
            channel.depth.log(len(channel.queue), t)
            channel.busy = False
            channel.utilization.log(0, t)
            channel.check_and_read(t, events)

            nt = newtime(t, FLAGS.ns_request, FLAGS.sdev_request)
            heappush(events, (nt, "P", core, lpa))

        elif c == "P":
            assert core.buffer[lpa] == False, pdb.set_trace()
            core.buffer[lpa] = True
            core.check_and_process(t, events)

        elif c == "F":
            assert core.busy == True, pdb.set_trace()
            assert core.buffer[lpa] == True, pdb.set_trace()
            core.busy = False
            core.utilization.log(0, t)
            del core.buffer[lpa]
            core.send_read_requests(t, events)
            core.check_and_process(t, events)
            finished[coreids[core]] += 1

            if len(core.buffer) == 0:
                assert len(core.waiting) == 0, pdb.set_trace()
                assert core.busy == False, pdb.set_trace()

                if i == len(lpas):
                    continue
                heappush(events,
                         (newtime(t, FLAGS.ns_request,
                                  FLAGS.sdev_request), "J", core, lpas[i]))
                i += 1

        else:
            assert False

    logging.info(str(len(lpas)))
    logging.info("finished = " + str(finished))
    logging.info("")

    for core in cores:
        print(core.utilization.average(t), end="\t")
    for i in range(0, 16 - len(cores)):
        print("", end="\t")
    for channel in channels:
        print(channel.utilization.average(t), end="\t")
    for channel in channels:
        print(channel.depth.average(t), end="\t")
    print()


if __name__ == "__main__":
    app.run(dist)
