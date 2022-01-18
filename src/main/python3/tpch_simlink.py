from absl import app, flags, logging
import os


def tpch_simlink(table_names, src_dir, dest_dir):
    if not os.path.exists(dest_dir):
        logging.info("Creating directory: %s!" % dest_dir)
        os.mkdir(dest_dir)
    for table_name in table_names:
        dest_table_dir = os.path.join(dest_dir, table_name)
        if os.path.exists(dest_table_dir):
            if os.path.islink(dest_table_dir):
                logging.warning("Symlink %s exists, delete and remake!" %
                                (dest_table_dir))
                os.remove(dest_table_dir)
            elif os.path.isdir(dest_table_dir):
                logging.warning("Dir %s exists, skipping!" % (dest_table_dir))
                continue
            else:
                logging.fatal("Unsupported file type of %s" % (dest_table_dir))

        src_table_dir = os.path.join(src_dir, table_name)
        os.symlink(src_table_dir, dest_table_dir, target_is_directory=True)


def main(argv):
    FLAGS = flags.FLAGS
    tpch_simlink(FLAGS.table_names, os.path.abspath(FLAGS.src_dir),
                 os.path.abspath(FLAGS.dest_dir))


if __name__ == "__main__":
    flags.DEFINE_string("src_dir", None, "Path to source directory")
    flags.DEFINE_string("dest_dir", None, "Path to destination directory")
    flags.DEFINE_list("table_names", [
        "customer", "lineitem", "nation", "orders", "part", "partsupp",
        "region", "supplier"
    ], "Table names")
    flags.mark_flag_as_required("src_dir")
    flags.mark_flag_as_required("dest_dir")

    app.run(main)
