#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import argparse
import json
import random
import subprocess
import sys
import time
from datetime import datetime

from iotlabcli.parser.common import nodes_id_list, nodes_list_from_info

from iotlabaggregator import connections, common
from iotlabaggregator.serial import SerialConnection, SerialAggregator


class NodeAggregator(connections.Aggregator):
    connection_class = SerialConnection

    def __init__(self, nodes_list, source, sink, sleep_time, packet_time, stops_at, *args, **kwargs):
        super(NodeAggregator, self).__init__(nodes_list, args, kwargs)

        #  TODO: Get the time limit for cutting experiments short from the experiment metadata
        #  The source and sink nodes
        self.source = source
        self.sink = sink
        self.packet_time = packet_time
        self.stops_at = stops_at
        self.sleep_time = sleep_time

        #  The failable nodes
        self.failables = [x for x in nodes_list if x != sink and x != source]
        #  The failed nodes as a tuple (node, time it returns)
        self.failed = []

    def run(self):  # overwrite original function
        #  Setup the sink node of the experiment
        self.send_nodes([self.sink], 'set sink\n')
        self.send_nodes([self.source], 'start\n')
        exp_start_time = time.time()

        while True:
            # Send a new message if we're past the duration of the old one
            if (time.time() - exp_start_time) >= self.packet_time:
                # self.send_nodes([self.source], 'start\n')
                exp_start_time = time.time()

            # Exit the experiment loop if the experiment needs to terminate
            # if datetime.now() >= self.stops_at:
            # break

            #  Fail nodes with a 1/5 chance each second
            if (random.randint(1, 5) % 5) == 0 and self.failables:
                failed_node = random.choice(self.failables)
                recovering_at = time.time() + self.sleep_time
                # self.send_nodes([failed_node], "sleep {}\n".format(self.sleep_time))

                self.failed.append((recovering_at, failed_node))
                self.failables.remove(failed_node)

                print "Failing node {}".format(failed_node)

            # Maintain the list of failed nodes
            recovered = []
            for up_time, node in self.failed:
                if time.time() >= up_time:
                    recovered.append((up_time, node))
                    print "{} coming back online".format(node)

            self.failed = [x for x in self.failed if x not in recovered]
            self.failables.extend(map(lambda x: x[1], recovered))

            # Loop this every second
            time.sleep(1.0 - (time.time() % 1.0))

        print "Experiment finished"


def main(argv):
    #  Parse the command line args
    parser = argparse.ArgumentParser(description="Control a deployment experiment on the IoT-LAB")
    parser.add_argument("id", metavar="id", nargs=1, help="The experiment ID to control")
    parser.add_argument("site", metavar="site", nargs=1, help="The deployment site")
    args = parser.parse_args(argv)

    #  Get experiment metadata
    metadata_raw = json.loads(subprocess.check_output(["iotlab-experiment", "get", "-l"]))
    if not metadata_raw['items']:
        print "No experiments listed"
        exit(2)

    for data in metadata_raw['items']:
        if data['id'] == int(args.id[0]):
            metadata = data

    log_file_str = metadata['name'] + '.log'

    # Get node list and decide src, sink nodes.
    nodes_raw = json.loads(subprocess.check_output(["iotlab-experiment", "get", "-i", args.id[0], "-ni"]))
    if not nodes_raw['items']:
        print "Experiment had no nodes assigned"
        exit(3)

    nodes = nodes_list_from_info(args.site[0], 'm3', nodes_raw['items'][0][args.site[0]]['m3'])
    stops_at = datetime.strptime(metadata['stop_date'], '%Y-%m-%dT%H:%M:%SZ')
    source, sink = random.sample(nodes, 2)

    with open(log_file_str, 'w') as log_file:
        #  Run `iotlab-experiment wait <id>` to wait for the experiment to start
        # subprocess.call(["iotlab-experiment", "wait", "-i", args.id[0], "--step", "1"])

        # Take the experiment start system time
        start_time = time.time()

        with NodeAggregator(nodes, source, sink, 15, 60, stops_at) as aggregator:
            aggregator = SerialAggregator(nodes)
            aggregator.run()

        # TODO: Start the serial logger

    """
    system time      ; id  ; message
    1588584749.859566;m3-99;[INFO: TPWSN-RMHB] Sending data beacon
    1588584755.860789;m3-99;[INFO: TPWSN-RMHB] Sending neighbour announce at time 1601
    1588584759.859094;m3-99;[INFO: TPWSN-RMHB] Sending data beacon
    1588584769.859343;m3-99;[INFO: TPWSN-RMHB] Sending data beacon
    1588584770.858889;m3-99;[INFO: TPWSN-RMHB] Sending neighbour announce at time 3101
    """


if __name__ == "__main__":
    main(["214061", "grenoble"])
    # main(sys.argv[1:])
