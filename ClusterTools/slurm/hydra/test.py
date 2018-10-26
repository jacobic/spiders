#!/usr/bin/env python
import os
import logging
from mpi4py import MPI
# multiprocess (pathos fork of multiprocessing) uses dill rather than pickle.
# This allows objects with loggers as attributes to be processed in parallel.
import multiprocess as mp
import tqdm
from src.utils import parallelize


def test(chunk):
    comm = MPI.COMM_WORLD
    current = mp.current_process()
    logging.info('rank {0.rank}, chunk {1}, process: {2.name} {2._identity}, '
                 'pid {3}'.format(comm, chunk, current, os.getpid()))


def parallelise(func, inputs, n_processes):
    with mp.Pool(processes=n_processes) as p:
        # Run multiprocessing with progress bar.
        # TODO: other bands can have (quicker) async map as order no longer
        # matters.
        r = list(tqdm.tqdm(p.imap(func, inputs), total=len(inputs)))
        # inform the processor that no new tasks will be added the pool
        p.close()
        # wait for map to be completed before proceeding
        p.join()


def main():
    comm = MPI.COMM_WORLD

    log_file, log_level = 'test.log', logging.DEBUG
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.captureWarnings(capture=True)
    logging.basicConfig(filename=log_file, level=log_level, format=log_format)
    logging.FileHandler(filename=log_file, mode='w')

    # Writes logging information to console.
    console_logger = logging.StreamHandler()
    console_logger.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
    logging.getLogger().addHandler(console_logger)

    if comm.rank == 0:
        data = range(0, 80)
        chunks = [[] for _ in range(comm.size)]
        for i, chunk in enumerate(data):
            chunks[i % comm.size].append(chunk)
    else:
        data = None
        chunks = None

    chunk = comm.scatter(chunks, root=0)
    logging.info(
        'rank {0.rank}, size {0.size}, len(chunk) {1}'.format(comm, len(chunk)))
    # print( 'rank {0.rank}, size {0.size}, len(chunk) {1}'.format(comm,
    # len(chunk)))

    parallelize(test, chunk, n_proc=20)


if __name__ == '__main__':
    main()
