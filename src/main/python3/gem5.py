from absl import app
from absl import flags
from absl import logging
import json
from multiprocessing import Pool
import os
import re

PREFIX_TABLE = {
    "c": "customer",
    "l": "lineitem",
    "n": "nation",
    "o": "orders",
    "ps": "partsupp",
    "p": "part",
    "r": "region",
    "s": "supplier"
}


def run(cmd):
    logging.info(cmd)
    os.system(cmd)


def gem5(query_dir, qid_re, desp_re, gem5_cmd, dryrun, num_process):
    cmds = []
    for d in os.listdir(query_dir):
        if not re.match(qid_re, d):
            continue
        for f in os.listdir(os.path.join(query_dir, d)):
            m = re.match(desp_re, f)
            if not m:
                continue
            m = m.groups()[0]
            json_path = os.path.join(query_dir, d, f)
            with open(json_path, "r") as ji:
                desp = json.load(ji)
                tprefix = desp[0]["name"].split("_")[0]
            qdir = os.path.join(query_dir, d)
            out_path = os.path.join(qdir, m + ".log")
            err_path = os.path.join(qdir, m + ".err")

            cmds.append(gem5_cmd % (qdir, m, PREFIX_TABLE[tprefix], json_path,
                                    out_path, err_path))

    if not dryrun:
        with Pool(num_process) as p:
            p.map(run, cmds)
    else:
        for cmd in cmds:
            logging.info(cmd)


def main(argv):
    dsuffix = ""
    if not flags.FLAGS.scratchpad:
        scratchpad = ""
        scratchpad_addr = 0
        dsuffix += "_noscratchpad"
    else:
        scratchpad = "--scratchpad --scratchpad_trace"
        scratchpad_addr = 1099511627776
        dsuffix += "_scratchpad"

    if not flags.FLAGS.streambuffer:
        streambuffer = ""
        streambuffer_addr = 0
        dsuffix += "_nostreambuffer"
    else:
        streambuffer = "--streambuffer --streambuffer_trace"
        streambuffer_addr = 1100585369600
        dsuffix += "_streambuffer"

    if not flags.FLAGS.prefetch:
        prefetch = ""
        dsuffix += "_noprefetch"
    else:
        prefetch = "--l1d-hwp-type=DCPTPrefetcher --l2-hwp-type=DCPTPrefetcher"
        dsuffix += "_DCPTPrefetcher"

    gem5_cmd = flags.FLAGS.gem5_cmd % (
        "%s", "%s", dsuffix, "%s %s %s" %
        (scratchpad, streambuffer, prefetch), "%s", "%s %d %d" %
        ("%s", scratchpad_addr, streambuffer_addr), "%s", "%s")

    gem5(flags.FLAGS.query_dir, flags.FLAGS.qid_re, flags.FLAGS.desp_re,
         gem5_cmd, flags.FLAGS.dryrun, flags.FLAGS.num_process)


if __name__ == "__main__":
    flags.DEFINE_string(
        "query_dir", os.path.join(os.getenv("COMPSTOR"), "tpch", "tbl_s1e1"),
        "Directory holding query description")
    flags.DEFINE_string("qid_re", r"Q([56]\d|7[012])$", "re for query id")
    flags.DEFINE_string("desp_re", r"^(\d+)\.json$",
                        "re for program description")
    flags.DEFINE_bool("prefetch", False, "prefetch")
    flags.DEFINE_bool("scratchpad", False, "scratchpad")
    flags.DEFINE_bool("streambuffer", False, "streambuffer")
    flags.DEFINE_string(
        "gem5_cmd", " ".join([
            os.path.join(os.getenv("HOME"),
                         "gem5/build/RISCV/gem5.opt"), "-d %s/m5_RISCV_%s%s",
            os.path.join(os.getenv("HOME"), "gem5/configs/example/se.py"),
            "--cpu-clock=2GHz --sys-clock=2GHz",
            "--mem-size=8192MB --cpu-type=TimingSimpleCPU --caches",
            "--l2cache --l1d_size=32kB --l1i_size=32kB --l2_size=256kB",
            "--l1d_assoc=8 --l1i_assoc=8 --l2_assoc=16 %s -c",
            os.path.join(os.getenv("COMPSTOR"),
                         "src/main/c/static/riscv_table_reader"), "-o", "\"",
            os.path.join(os.getenv("COMPSTOR"), "tpch/tbl_s1e1/%s/00000.tbl"),
            "%s", "\"", "> %s", "2> %s"
        ]), "gem5 command format")
    flags.DEFINE_bool("dryrun", True, "dryrun")
    flags.DEFINE_integer("num_process",
                         len(os.sched_getaffinity(0)) // 2, "num processes")
    app.run(main)
