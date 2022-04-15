from absl import app
from absl import flags
from absl import logging
import json
from multiprocessing import Pool
import os
import pdb
import re

FIRST_TABLE_SIZE = {
    "c": 134217656,
    "l": 134217706,
    "n": 2224,
    "o": 134217645,
    "ps": 134217628,
    "p": 134217657,
    "r": 389,
    "s": 14176368,
}

TOTAL_TABLE_SIZE = {
    "c": 244847642,
    "l": 7775727688,
    "n": 2224,
    "o": 1749195031,
    "ps": 1204850769,
    "p": 243336157,
    "r": 389,
    "s": 14176368,
}

GEM5_VARIANTS = {
    "nnn": "noscratchpad_nostreambuffer_noprefetch",
    "nnd": "noscratchpad_nostreambuffer_DCPTPrefetcher",
    "ssn": "scratchpad_streambuffer_noprefetch",
    "ssd": "scratchpad_streambuffer_DCPTPrefetcher",
}


def get_cycle_bytes(stats_path, cycle_re, bytes_re):
    cycle = 0
    iobytes = 0
    logging.info(stats_path)

    with open(stats_path, "r") as si:
        for line in si:
            line = line.strip()
            m = re.match(cycle_re, line)
            if m:
                cycle += int(m.groups()[0])

            m = re.match(bytes_re, line)
            if m:
                iobytes += int(m.groups()[0])

    return cycle, iobytes


def analysis(query_dir, qid_re, desp_re, stats_format, cycle_re, bytes_re):
    result = {}
    for d in os.listdir(query_dir):
        if not re.match(qid_re, d):
            continue

        result[d] = {k: 0 for k in GEM5_VARIANTS}
        result[d]["bytes"] = 0

        for f in os.listdir(os.path.join(query_dir, d)):
            m = re.match(desp_re, f)
            if not m:
                continue
            json_path = os.path.join(query_dir, d, f)
            with open(json_path, "r") as ji:
                desp = json.load(ji)
                prefix = desp[0]["name"].split("_")[0]

            for k, v in GEM5_VARIANTS.items():
                stats_path = os.path.join(
                    query_dir, d, stats_format % (int(m.groups()[0]), v))
                if (not os.path.exists(stats_path)):
                    logging.error("%s does not exists" % stats_path)
                    continue
                cycle, iobytes = get_cycle_bytes(stats_path, cycle_re,
                                                 bytes_re)

                result[d][k] += (cycle * TOTAL_TABLE_SIZE[prefix] //
                                 FIRST_TABLE_SIZE[prefix])

                if (k == "ssn"):
                    result[d]["bytes"] += (iobytes *
                                           TOTAL_TABLE_SIZE[prefix] //
                                           FIRST_TABLE_SIZE[prefix])

    return result


def main(argv):
    F = flags.FLAGS
    result = analysis(F.query_dir, F.qid_re, F.desp_re, F.stats_format,
                      F.cycle_re, F.bytes_re)

    for k in list(GEM5_VARIANTS.keys()) + ["bytes"]:
        print("\t%s" % k, end="")
    for d in sorted(result.keys()):
        print("\n" + d, end="")
        for k in list(GEM5_VARIANTS.keys()) + ["bytes"]:
            print("\t%d" % result[d][k], end="")
    print()


if __name__ == "__main__":
    flags.DEFINE_string(
        "query_dir",
        os.path.join(
            os.path.split(
                os.path.split(os.path.split(
                    os.path.split(__file__)[0])[0])[0])[0], "tpch",
            "tbl_s1e1"), "Directory holding query description")
    flags.DEFINE_string("qid_re", r"Q([567]\d)$", "re for query id")
    flags.DEFINE_string("desp_re", r"^(\d+)\.json$",
                        "re for program description")
    flags.DEFINE_string("stats_format", r"m5_RISCV_%d_%s/stats.txt",
                        "stats format")
    flags.DEFINE_string("cycle_re", r"system\.cpu\.numCycles\s+(\d+)",
                        "cycle re")
    flags.DEFINE_string("bytes_re",
                        r"system\.cpu\.streambuffer\.bytes\w+::total\s+(\d+)",
                        "bytes re")
    app.run(main)
