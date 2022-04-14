# Potential benefits for variable-width elements

from absl import app
from absl import flags
from absl import logging
import datetime
import decimal
import json
import numpy
import os
import pdb
import pyarrow
import pyarrow.compute
import pyarrow.dataset
import re
import sys

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

EPOCH = datetime.date(1970, 1, 1)
WIDTHS = (1, 2, 4, 8, 16, 24, 32, 40, 48, 56, 62)

# 2 EqualTo
# 3 NotEqualTo
# 4 GreaterThan
# 5 GreaterThanOrEqual
# 6 LessThan
# 7 LessThanOrEqual

# 0 int
# 1 date
# 2 decimal
# 3 str
# 4 scanner


def stat(job, data):
    desp, table, d = job
    logging.info("%s_%s" % (d, table))

    ans = None
    for column in desp:
        if column["required"] == (column["type"] == 4):
            logging.error(str(column))
        if column["type"] > 2:
            continue
        if not column["required"]:
            continue

        ops = len(column["filters"])
        datacol = data[table][column["name"]]

        count = []
        for w in WIDTHS:
            if (column["type"] == 2):
                threshold = pyarrow.scalar(
                    decimal.Decimal(str((1 << w) / (10**datacol.type.scale))),
                    pyarrow.decimal128(36, datacol.type.scale))
            elif (column["type"] == 1):
                threshold = pyarrow.scalar(
                    EPOCH + datetime.timedelta(days=(1 << min(w, 20))),
                    pyarrow.date32())
            else:
                assert (column["type"] == 0)
                threshold = pyarrow.scalar(1 << w, pyarrow.int64())

            bs = pyarrow.compute.less(datacol, threshold)
            count.append(bs.combine_chunks().true_count)

        count = numpy.array(count)
        ops = count * ops

        if ans is None:
            ans = numpy.concatenate((count, ops))
        else:
            ans += numpy.concatenate((count, ops))

    return ans


def jsons(query_dir, qid_re, desp_re):
    ans = []
    for d in os.listdir(query_dir):
        if not re.match(qid_re, d):
            continue
        for f in os.listdir(os.path.join(query_dir, d)):
            m = re.match(desp_re, f)
            if not m:
                continue
            # m = m.groups()[0]
            json_path = os.path.join(query_dir, d, f)
            with open(json_path, "r") as ji:
                desp = json.load(ji)
                tprefix = desp[0]["name"].split("_")[0]

            ans.append([desp, PREFIX_TABLE[tprefix], d])
    return ans


def main(argv):
    FLAGS = flags.FLAGS
    jobs = jsons(FLAGS.query_dir, FLAGS.qid_re, FLAGS.desp_re)

    data = {}
    for tname in PREFIX_TABLE.values():
        logging.info("Loading table %s" % tname)
        data[tname] = pyarrow.dataset.dataset(
            os.path.join(FLAGS.data_dir, tname)).to_table()

    logging.info("Tables %s loaded!" % str(data.keys()))

    stats = {}
    for job in jobs:
        count = stat(job, data)
        if job[2] in stats:
            stats[job[2]] += count
        else:
            stats[job[2]] = count

    fo = sys.stdout

    fo.write("\n")
    for q in sorted(stats.keys()):
        fo.write("%s" % q)
        for c in stats[q]:
            fo.write("\t%d" % c)
        fo.write("\n")


if __name__ == "__main__":
    flags.DEFINE_string(
        "query_dir", os.path.join(os.getenv("COMPSTOR"), "tpch", "tbl_s1e1"),
        "Directory holding query description")
    flags.DEFINE_string("qid_re", r"Q([56]\d|7[012])$", "re for query id")
    flags.DEFINE_string("desp_re", r"^(\d+)\.json$",
                        "re for program description")
    flags.DEFINE_string(
        "data_dir",
        os.path.join(os.getenv("COMPSTOR"), "tpch", "parquet_s1e1_data"),
        "Directory holding parquet data")
    flags.DEFINE_integer("num_process",
                         len(os.sched_getaffinity(0)) // 2, "num processes")

    app.run(main)
