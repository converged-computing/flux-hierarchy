#!/usr/bin/env python

import argparse
import os
import sys

import flux_hierarchy
from flux_hierarchy.logger import setup_logger


def get_parser():
    parser = argparse.ArgumentParser(
        description="Flux Hierarchy",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    # Global Variables
    parser.add_argument(
        "--debug",
        dest="debug",
        help="use verbose logging to debug.",
        default=False,
        action="store_true",
    )

    parser.add_argument(
        "--quiet",
        dest="quiet",
        help="suppress additional output.",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--version",
        dest="version",
        help="show software version.",
        default=False,
        action="store_true",
    )

    subparsers = parser.add_subparsers(
        help="actions",
        title="actions",
        description="actions",
        dest="command",
    )
    subparsers.add_parser("version", description="show software version")

    # start an instane hierarchy
    start = subparsers.add_parser(
        "start",
        formatter_class=argparse.RawTextHelpFormatter,
        description="start an instance hierarchy",
    )
    start.add_argument("-o", "--out", help="output directory for hierarchy assets", default=None)

    view = subparsers.add_parser(
        "view",
        formatter_class=argparse.RawTextHelpFormatter,
        description="view an instance hierarchy",
    )

    # Maybe this warrants a better name, but this seems to be what we'd want to do -
    throughput = subparsers.add_parser(
        "throughput",
        formatter_class=argparse.RawTextHelpFormatter,
        description="Run job throughput test on a Flux Hierarchy",
    )
    throughput.add_argument("-p", "--prefix", help="Prefix for the group (used for kvs)")
    throughput.add_argument(
        "-n", "--njobs", type=int, metavar="N", help="Total number of jobs to run", default=100
    )
    throughput.add_argument(
        "-t", "--runtime", help="Simulated runtime of each job (default=1ms)", default="0.001s"
    )
    throughput.add_argument(
        "--local", help="Submit to local URIs (do not use ssh)", action="store_true"
    )
    throughput.add_argument(
        "-x", "--exec", help="Do not simulate, actually run jobs", action="store_true"
    )
    throughput.add_argument(
        "-o", "--setopt", action="append", help="Set shell option OPT or OPT=VAL", metavar="OPT"
    )
    throughput.add_argument("-k", "--keep-env", help="Do not clean environment.", dest="keep_env")
    throughput.add_argument(
        "--setattr", action="append", help="Set job attribute ATTR=VAL", metavar="ATTR=VAL"
    )
    throughput.add_argument("execute", nargs=argparse.REMAINDER, default=["true"])
    throughput.add_argument("--outdir", help="Output directory")
    throughput.add_argument(
        "--keep", help="Keep hierarchy running (do not stop it)", action="store_true"
    )
    throughput.add_argument(
        "--no-cleanup", help="Given stop, do NOT cleanup", action="store_true", dest="skip_cleanup"
    )

    for cmd in [start, throughput, view]:
        cmd.add_argument("config", help="hierachy description YAML file")

    return parser


def run():
    """
    this is the main entrypoint.
    """
    parser = get_parser()

    def help(return_code=0):
        version = flux_hierarchy.__version__

        print("\nFlux Hierarchy v%s" % version)
        parser.print_help()
        sys.exit(return_code)

    # If the user didn't provide any arguments, show the full help
    if len(sys.argv) == 1:
        help()

    # If an error occurs while parsing the arguments, the interpreter will exit with value 2
    args, extra = parser.parse_known_args()

    if args.debug is True:
        os.environ["MESSAGELEVEL"] = "DEBUG"

    # Show the version and exit
    if args.command == "version" or args.version:
        print(flux_hierarchy.__version__)
        sys.exit(0)

    setup_logger(
        quiet=args.quiet,
        debug=args.debug,
    )

    # Here we can assume instantiated to get args
    if args.command == "throughput":
        from .throughput import main
    elif args.command == "start":
        from .start import main
    elif args.command == "view":
        from .view import main
    else:
        help(1)
    main(args, extra)


if __name__ == "__main__":
    run()
