import os
import threading
from enum import Enum

import boto3

from argus.client.generic_result import Status
from mgmt_cli_test import ManagerTestFunctionsMixIn
from sdcm import mgmt
from sdcm.argus_results import ManagerBackupReadResult, ManagerBackupBenchmarkResult, submit_results_to_argus
from sdcm.cluster import BaseNode
from sdcm.remote import shell_script_cmd
from sdcm.utils.node import build_node_api_command, RequestMethods
from sdcm.utils.time_utils import ExecutionTimer


class ManagerReportType(Enum):
    READ = 1
    BACKUP = 2


def format_size(size_in_bytes):
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
        if size_in_bytes < 1024:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024


class ManagerBackupRestoreConcurrentTests(ManagerTestFunctionsMixIn):
    snapshot_list_lock = threading.Lock()
    snapshot_ids = {}
    node_sstables = {}
    node_backup_size = {}
    base_prefix = ""

    def report_to_argus(self, report_type: ManagerReportType, data: dict, label: str):
        if report_type == ManagerReportType.READ:
            table = ManagerBackupReadResult(sut_timestamp=mgmt.get_scylla_manager_tool(
                manager_node=self.monitors.nodes[0]).sctool.client_version_timestamp)
        elif report_type == ManagerReportType.BACKUP:
            table = ManagerBackupBenchmarkResult(sut_timestamp=mgmt.get_scylla_manager_tool(
                manager_node=self.monitors.nodes[0]).sctool.client_version_timestamp)
        else:
            raise ValueError("Unknown report type")

        for key, value in data.items():
            table.add_result(column=key, value=value, row=label, status=Status.UNSET)
        submit_results_to_argus(self.test_config.argus_client(), table)

    def set_balancing(self, balancing: bool):
        for node in self.db_cluster.nodes:
            balancing_cmd = build_node_api_command(
                f'/storage_service/tablets/balancing?enabled={"true" if balancing else "false"}', RequestMethods.POST)
            result = node.remoter.run(balancing_cmd, ignore_status=True, verbose=True)

    # actual tests
    def test_native_restore_from_backup(self):
        self.log.info("Executing test_backup_restore_benchmark...")

        for node in self.db_cluster.nodes:
            node.remoter.sudo(shell_script_cmd("""\
                echo '\nobject_storage_endpoints:\n  - name: s3.us-east-1.amazonaws.com\n    port: 443\n    https: true\n    aws_region: us-east-1\n    iam_role_arn: arn:aws:iam::797456418907:instance-profile/qa-scylla-manager-backup-instance-profile\n' >> /etc/scylla/scylla.yaml
                    """))
            node.restart_scylla_server()

        self.db_cluster.nodes[0].run_cqlsh(
            '''CREATE KEYSPACE keyspace1 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'enabled': true};''')
        self.db_cluster.nodes[0].run_cqlsh('''CREATE TABLE keyspace1.standard1
                                              (
                                                  key  blob,
                                                  "C0" blob,
                                                  PRIMARY KEY (key)
                                              ) WITH bloom_filter_fp_chance = 0.01
                                                    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
                                                    AND comment = ''
                                                    AND compaction = {'class': 'IncrementalCompactionStrategy'}
                                                    AND compression = {}
                                                    AND crc_check_chance = 1
                                                    AND default_time_to_live = 0
                                                    AND gc_grace_seconds = 864000
                                                    AND max_index_interval = 2048
                                                    AND memtable_flush_period_in_ms = 0
                                                    AND min_index_interval = 128
                                                    AND speculative_retry = '99.0PERCENTILE'
                                                    AND tombstone_gc = {'mode': 'disabled'};
                                           ''')

        node_directories = ["9c3768b6-d9d7-11f0-9a06-0215e3da214b", "9c9eb8a4-d9d7-11f0-9ff1-02599936cbbf",
                            "9d3675a4-d9d7-11f0-9d23-0246fe8624d5", "9d682e96-d9d7-11f0-9781-029e2ba14dc7",
                            "9dc5722c-d9d7-11f0-8a7c-025591442401", "9e3415d8-d9d7-11f0-9ba6-0270b0040487"]

        def list_and_distribute_toc_files(bucket, prefix, node_ids):
            """
            Returns a map: {node_id: [toc filenames]}, where:
              * TOC files come from S3 under the given prefix
              * Each TOC file is paired with its corresponding -big-Data.db file
              * Sorted by Data.db size (largest first)
              * Distributed round-robin across nodes
            """
            s3 = boto3.client("s3")
            paginator = s3.get_paginator("list_objects_v2")

            toc_entries = []  # list of tuples: (toc_filename, data_size)

            # First pass: collect all objects
            all_objects = []
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                all_objects.extend(page.get("Contents", []))

            # Build a lookup: basename → size
            size_map = {os.path.basename(obj["Key"]): obj["Size"] for obj in all_objects}

            # Extract TOC files and match them with Data.db sizes
            for obj in all_objects:
                key = obj["Key"]
                if key.endswith("-big-TOC.txt"):
                    toc_name = os.path.basename(key)
                    data_name = toc_name.replace("-big-TOC.txt", "-big-Data.db")

                    data_size = size_map.get(data_name, 0)  # fallback if missing
                    toc_entries.append((toc_name, data_size))

            # Sort by Data.db size (largest first)
            toc_entries.sort(key=lambda x: x[1], reverse=True)

            # Round-robin distribution
            result = {node_id: [] for node_id in node_ids}
            idx = 0
            for toc_name, _ in toc_entries:
                node_id = node_ids[idx % len(node_ids)]
                result[node_id].append(toc_name)
                idx += 1

            return result

        def build_full_toc_map(bucket, base_prefix, node_directories, node_ids):
            """
            Returns:
              {
                node_id: {
                    dir1: [toc1, toc2, ...],
                    dir2: [...],
                    ...
                }
              }
            """
            # Initialize structure
            full_map = {node_id: {} for node_id in node_ids}

            for s3_dir in node_directories:
                prefix = f"{base_prefix}/{s3_dir}"

                per_dir_map = list_and_distribute_toc_files(
                    bucket=bucket,
                    prefix=prefix,
                    node_ids=node_ids
                )

                # Merge into full_map
                for node_id in node_ids:
                    full_map[node_id][s3_dir] = per_dir_map[node_id]

            return full_map

        node_ids = [node.uuid for node in self.db_cluster.nodes]

        toc_map = build_full_toc_map(
            bucket="manager-backup-tests-permanent-snapshots-us-east-1",
            base_prefix="ernest-sct-tests/6TB-tablets-RF3-6node",
            node_directories=node_directories,
            node_ids=node_ids
        )

        def restore(scylla_node: BaseNode, s3_dir: str, toc_list: list[str]):
            # Create a temporary file with the TOC list
            filename = f"/tmp/{scylla_node.uuid}-{s3_dir}-sstables.txt"

            with open(filename, "w") as f:
                f.write("\n".join(toc_list))
            self.log.info(
                f"FOOOBAR: Restoring on node {scylla_node.uuid} from TOC file {filename} with contents:\n{toc_list}")
            scylla_node.remoter.send_files(src=filename, dst=filename)

            # Run restore for this specific directory
            res = scylla_node.run_nodetool(
                f"restore "
                f"--primary-replica-only true "
                f"--endpoint s3.us-east-1.amazonaws.com "
                f"--bucket manager-backup-tests-permanent-snapshots-us-east-1 "
                f"--scope all "
                f"--prefix ernest-sct-tests/6TB-tablets-RF3-6node/{s3_dir} "
                f"--keyspace keyspace1 --table standard1 "
                f"--sstables-file-list {filename}"
            )

            if res is not None and res.exit_status != 0:
                raise Exception(f"Restore failed: {res.stdout}")

        self.set_balancing(False)
        with ExecutionTimer() as lns_timer:
            # Transpose: iterate over directories first
            for s3_dir in node_directories:
                restore_threads = []

                for node in self.db_cluster.nodes:
                    node_uuid = node.uuid
                    toc_list = toc_map[node_uuid].get(s3_dir, [])

                    if not toc_list:
                        continue

                    thread = threading.Thread(
                        target=restore,
                        args=(node, s3_dir, toc_list)
                    )
                    restore_threads.append(thread)
                    thread.start()

                # Wait for all nodes to finish restoring this directory
                for thread in restore_threads:
                    thread.join()


        restore_report = {
            # "Size": format_size(sum(self.node_backup_size.values()) / len(self.node_backup_size.values())),
            "Size": format_size(3000000000),
            "Time": int(lns_timer.duration.total_seconds()),
        }

        self.report_to_argus(ManagerReportType.BACKUP, restore_report, "nodetool restore")
