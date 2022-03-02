from absl import app
from absl import flags
from absl import logging
import os
import re
import subprocess
import time

FLAGS = flags.FLAGS


def run_query(qid):
    DIR = os.path.dirname(os.path.realpath(__file__))
    COMPSTOR = os.getenv("COMPSTOR")

    SPARK_MASTER_URL = "--master local[32]".split()
    CORE_MEM_CONF = ("--driver-memory 32g --executor-memory 64g "
                     "--executor-cores 32 --conf spark.executor.memory=64g "
                     "--conf spark.sql.broadcastTimeout=1000").split()
    STRING_TRUNCATE_LIMIT = (
        "--conf spark.sql.debug.maxToStringFields=10000 "
        "--conf spark.debug.maxToStringFields=10000 "
        "--conf spark.sql.maxMetadataStringLength=10000").split()
    LIBRARY_PATH_CONF = " ".join((
        "--conf spark.driver.extraLibraryPath=%s/lib" % COMPSTOR,
        "--conf spark.executor.extraLibraryPath=%s/lib" % COMPSTOR,
    )).split()
    EXECUTOR_ENV = ""

    EVENTLOG_CONF = ("--conf spark.eventLog.enabled=true "
                     "--conf spark.eventLog.dir=%s/tpch/spark-events" %
                     COMPSTOR).split()
    DEBUG_CONF = (
        "--conf spark.driver.extraJavaOptions=-agentlib:"
        "jdwp=transport=dt_socket,server=y,suspend=y,address=5005").split()

    command = [
        "%s/spark-3.1.2-bin-hadoop3.2/bin/spark-submit" % COMPSTOR, "--class",
        "tpch.TpchQuery"
    ]
    command += SPARK_MASTER_URL
    command += CORE_MEM_CONF
    command += STRING_TRUNCATE_LIMIT
    command += LIBRARY_PATH_CONF
    command += EXECUTOR_ENV
    command += EVENTLOG_CONF
    if FLAGS.debug:
        command += DEBUG_CONF
    command += [
        "%s/target/scala-2.12/riscvstorage_2.12-0.1.0.jar" % COMPSTOR,
        FLAGS.input_dir, FLAGS.output_dir,
        str(qid)
    ]

    drop_pagecache = os.path.join(os.getenv("COMPSTOR"),
                                  "target/bin/drop_pagecache")
    os.system(drop_pagecache)

    qid_int = int(qid)
    qid_dir = os.path.join(FLAGS.output_dir, "Q%02d" % qid_int)
    if not os.path.isdir(qid_dir):
        os.mkdir(qid_dir)
    logging.info(command)
    with open(os.path.join(FLAGS.output_dir, "Q%02d.json" % qid_int),
              "w") as fout:
        with open(os.path.join(FLAGS.output_dir, "Q%02d.err.txt" % qid_int),
                  "w") as ferr:
            subprocess.run(command,
                           stdout=fout,
                           stderr=ferr,
                           env={
                               "JAVA_HOME": os.getenv("JAVA_HOME"),
                               "HOME": os.getenv("HOME"),
                               "COMPSTOR": os.getenv("COMPSTOR"),
                               "PYTHONPATH": os.getenv("PYTHONPATH"),
                           })
    time.sleep(1)
    os.system("mv /tmp/*.json %s" % qid_dir)
    os.system("mv %s/*.csv %s" % (FLAGS.output_dir, qid_dir))


def main(argv):
    if not os.path.isdir(FLAGS.input_dir):
        logging.fatal("%s does not exist" % FLAGS.input_dir)
    if FLAGS.compile:
        os.chdir(os.getenv("COMPSTOR"))
        rc = subprocess.call(["sbt", "package"])
        if (rc != 0):
            logging.fatal("sbt package failed")
    if not os.path.isdir(FLAGS.output_dir):
        logging.info("Creating %s" % FLAGS.output_dir)
        os.mkdir(FLAGS.output_dir)
    os.chdir(FLAGS.output_dir)
    if FLAGS.qid is None:
        if FLAGS.tbl2parquet:
            run_query(0)
        elif FLAGS.tbl_source:
            for qid in range(76, 98):
                run_query(qid)
        elif FLAGS.riscv_source:
            for qid in range(51, 73):
                run_query(qid)
        else:
            for qid in range(1, 23):
                run_query(qid)
    else:
        run_query(FLAGS.qid)


if __name__ == "__main__":
    flags.DEFINE_bool("compile",
                      False,
                      "Compile before submit",
                      short_name='c')
    flags.DEFINE_bool("debug", False, "Debug", short_name="d")
    flags.DEFINE_bool("tbl_source", False, "table source", short_name="t")
    flags.DEFINE_bool("riscv_source", False, "riscv source", short_name="r")
    flags.DEFINE_bool("tbl2parquet",
                      False,
                      "tbl to parquet conversion",
                      short_name="p")
    flags.DEFINE_integer("qid", None, "Query id", short_name="q")
    flags.DEFINE_string("input_dir",
                        os.path.join(os.getenv("COMPSTOR"), "tpch",
                                     "tbl_s1e1"),
                        "input dir",
                        short_name="-i")
    flags.DEFINE_string("output_dir",
                        os.path.join(os.getenv("COMPSTOR"), "tpch",
                                     "tbl_s1e1"),
                        "output dir",
                        short_name="-o")
    app.run(main)
