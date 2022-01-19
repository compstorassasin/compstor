from absl import app
from absl import flags
from absl import logging
import os
import pdb
import re


def partitioner(table_name, table_path, part_size, block_size, sep):
    part_dir = os.path.join(os.path.split(table_path)[0], table_name)
    if os.path.exists(part_dir):
        logging.warning("%s exists, skipping ..." % part_dir)
        return

    os.mkdir(part_dir)
    rest_size = os.path.getsize(table_path)
    part_id = 0

    part_sizes = []

    with open(table_path, "rb") as ti:
        while (rest_size):
            if rest_size < part_size:
                this_size = rest_size
            else:
                ti.seek(part_size, 1)
                this_size = part_size
                found = False
                for i in range(part_size // block_size):
                    this_size -= block_size
                    ti.seek(-block_size, 1)
                    s = ti.read(block_size)
                    ti.seek(-block_size, 1)
                    for j in range(block_size - 1, -1, -1):
                        if s[j] == ord(sep):
                            found = True
                            ti.seek(-this_size, 1)
                            this_size += (j + 1)
                            break
                    if found:
                        break
                assert found, pdb.set_trace()
            part_sizes.append(this_size)
            s = ti.read(this_size)
            with open(
                    os.path.join(os.path.join(part_dir, "%05d.tbl" % part_id)),
                    "wb") as fo:
                fo.write(s)
            part_id += 1
            rest_size -= this_size

    assert os.path.getsize(table_path) == sum(part_sizes), pdb.set_trace()
    with open(os.path.join(part_dir, ".meta"), "w") as fo:
        fo.write("%d" % len(part_sizes))
        for i in range(len(part_sizes)):
            if (i == 0):
                fo.write("\n%d" % part_sizes[i])
            else:
                fo.write(" %d" % part_sizes[i])
        fo.write("\n")


def tpch_partitioner(table_dir, table_re, part_size, block_size, sep):
    for t in os.listdir(table_dir):
        m = re.match(table_re, t)
        if not m:
            continue
        table_path = os.path.join(table_dir, t)
        partitioner(m.groups()[0], table_path, part_size, block_size, sep)


def main(argv):
    FLAGS = flags.FLAGS
    tpch_partitioner(FLAGS.table_dir, FLAGS.table_re, FLAGS.part_size,
                     FLAGS.block_size, FLAGS.sep)


if __name__ == "__main__":
    flags.DEFINE_string(
        "table_dir",
        os.path.join(os.getenv("COMPSTOR"), "tpch", "tbl_s1e1_data"),
        "directory holding the tables")
    flags.DEFINE_string("table_re", "(\w+)\.tbl", "table re")
    flags.DEFINE_integer("part_size", 128 << 20, "part size")
    flags.DEFINE_integer("block_size", 256, "block size")
    flags.DEFINE_string("sep", "\n", "seperator")
    app.run(main)
