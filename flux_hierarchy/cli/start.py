#!/usr/bin/env python

from flux_hierarchy import FluxHierarchy


def main(args, _):
    """
    Start a set of nested instances to submit jobs to.
    """
    hier = FluxHierarchy(args.config, outdir=args.out)
    # Note that this returns the uri lookup, if we wanted
    # to use it somewhere.
    hier.start()
