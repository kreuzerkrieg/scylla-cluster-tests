import json
import os
import re
import threading
import uuid
from enum import Enum
from time import sleep

from argus.client.generic_result import Status
from mgmt_cli_test import ManagerTestFunctionsMixIn
from sdcm import mgmt
from sdcm.argus_results import ManagerBackupReadResult, ManagerBackupBenchmarkResult, submit_results_to_argus
from sdcm.cluster import BaseNode
from sdcm.mgmt import TaskStatus
from sdcm.remote import shell_script_cmd
from sdcm.rest.remote_curl_client import RemoteCurlClient
from sdcm.rest.storage_service_client import StorageServiceClient
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.node import build_node_api_command, RequestMethods
from sdcm.utils.time_utils import ExecutionTimer


class ManagerReportType(Enum):
    READ = 1
    BACKUP = 2


def parse_backup_size(mgr_cluster, task_id):
    res = mgr_cluster.sctool.run(cmd=f"progress {task_id} -c {mgr_cluster.id}",
                                 parse_table_res=False)
    match = re.search(r".+100% │ (.*?) │ ", res.stdout, re.MULTILINE)
    if match:
        return match.group(1)
    else:
        raise ValueError(f"Backup size not found in the output in {res.stdout}")


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

    def create_backup_and_report(self, mgr_cluster, label: str):
        InfoEvent(message='Starting `rclone` based backup').publish()
        task = mgr_cluster.create_backup_task(location_list=self.locations, rate_limit_list=["0"])

        backup_status = task.wait_and_get_final_status(timeout=72000)
        assert backup_status == TaskStatus.DONE, "Backup upload has failed!"

        backup_report = {
            "Size": parse_backup_size(mgr_cluster, task.id),
            "Time": int(task.duration.total_seconds()),
        }
        self.report_to_argus(ManagerReportType.BACKUP, backup_report, label)
        return task

    def create_restore_and_report(self, mgr_cluster, snapshot_tag, label: str):
        self.set_balancing(False)

        task = self.restore_backup_with_task(mgr_cluster=mgr_cluster, snapshot_tag=snapshot_tag,
                                             timeout=72000, restore_data=True)

        backup_status = task.wait_and_get_final_status(timeout=72000)
        assert backup_status == TaskStatus.DONE, "Backup upload has failed!"

        backup_report = {
            "Size": parse_backup_size(mgr_cluster, task.id),
            "Time": int(task.duration.total_seconds()),
        }
        self.report_to_argus(ManagerReportType.BACKUP, backup_report, label)
        self.set_balancing(True)
        return task

    def run_read_stress_and_report(self, label):
        stress_queue = {"read": [], "write": []}

        for command in self.params.get('stress_cmd'):
            if " write " in command:
                stress_queue["write"].append(self.run_stress_thread(command))
            elif " read " in command:
                stress_queue["read"].append(self.run_stress_thread(command))
            else:
                raise ValueError("Unknown stress command")

        def get_stress_averages(queue):
            averages = {'op rate': 0.0, 'partition rate': 0.0, 'row rate': 0.0, 'latency 99th percentile': 0.0}
            num_results = 0
            for stress in queue:
                results = self.get_stress_results(queue=stress)
                num_results += len(results)
                for result in results:
                    for key in averages:
                        averages[key] += float(result[key])
            stats = {key: averages[key] / num_results for key in averages}
            return stats

        with ExecutionTimer() as stress_timer:
            read_stats = get_stress_averages(stress_queue["read"])
            write_stats = get_stress_averages(stress_queue["write"])

        InfoEvent(message=f'Read stress duration: {stress_timer.duration}s.').publish()

        read_stress_report = {
            "read time": int(stress_timer.duration.total_seconds()),
            "op rate": read_stats['op rate'],
            "partition rate": read_stats['partition rate'],
            "row rate": read_stats['row rate'],
            "latency 99th percentile": read_stats['latency 99th percentile'],
        }
        self.report_to_argus(ManagerReportType.READ, read_stress_report, "Read stress: " + label)

        write_stress_report = {
            "read time": int(stress_timer.duration.total_seconds()),
            "op rate": write_stats['op rate'],
            "partition rate": write_stats['partition rate'],
            "row rate": write_stats['row rate'],
            "latency 99th percentile": write_stats['latency 99th percentile'],
        }
        self.report_to_argus(ManagerReportType.READ, write_stress_report, "Write stress: " + label)

    def backup(self, scylla_node: BaseNode):
        scylla_node.run_nodetool("flush")
        result = scylla_node.run_nodetool('snapshot')
        snapshot_name = re.findall(r'(\d+)', result.stdout.split("snapshot name")[1])[0]
        with self.snapshot_list_lock:
            self.snapshot_ids[scylla_node.uuid] = snapshot_name
            manifest_path = scylla_node.remoter.sudo(
                f"find /var/lib/scylla/data/keyspace1/standard1-*/snapshots/{snapshot_name}/ -name manifest.json",
                ignore_status=True, verbose=True)
            manifest_content = scylla_node.remoter.sudo(f"cat {manifest_path.stdout}", ignore_status=True,
                                                        verbose=True).stdout

            parsed_data = json.loads(manifest_content)
            file_list = parsed_data['files']
            self.node_sstables[scylla_node.uuid] = [file_name.replace("-Data.db", "-TOC.txt") for file_name in
                                                    file_list]

            backup_size_res = scylla_node.remoter.sudo(
                f"du -sb /var/lib/scylla/data/keyspace1/standard1-*/snapshots/{snapshot_name}/")
            if backup_size_res.stdout:
                self.node_backup_size[scylla_node.uuid] = int(
                    backup_size_res.stdout[:backup_size_res.stdout.find("\t")])

        backup_res = scylla_node.run_nodetool(
            f"backup --endpoint s3.us-east-1.amazonaws.com --bucket manager-backup-tests-us-east-1 --prefix {self.base_prefix}/{scylla_node.uuid} --keyspace keyspace1 --table standard1 --snapshot {snapshot_name}")
        if backup_res is not None and backup_res.exit_status != 0:
            raise Exception(f"Backup failed: {backup_res.stdout}")

    def backup_and_report(self, label):
        self.snapshot_ids = {}
        self.node_sstables = {}
        self.node_backup_size = {}
        self.base_prefix = f"standard1/keyspace1/{str(uuid.uuid4())}"
        backup_threads = []
        with ExecutionTimer() as backup_timer:
            for node in self.db_cluster.nodes:
                thread = threading.Thread(target=self.backup, args=(node,))
                backup_threads.append(thread)
                thread.start()
            for thread in backup_threads:
                thread.join()
        backup_report = {
            "Size": format_size(sum(self.node_backup_size.values()) / len(self.node_backup_size.values())),
            "Time": int(backup_timer.duration.total_seconds()),
        }
        self.report_to_argus(ManagerReportType.BACKUP, backup_report, label)

    def restore(self, scylla_node: BaseNode):
        sstables_list = []
        for toc in self.node_sstables[scylla_node.uuid]:
            sstables_list.append(os.path.basename(toc))

        text = "\n".join(sstables_list)
        filename = f'/tmp/{scylla_node.uuid}-sstables.txt'
        with open(filename, "w") as file:
            file.write(text)

        scylla_node.remoter.send_files(src=filename, dst=filename)
        res = scylla_node.run_nodetool(
            f"restore --endpoint s3.us-east-1.amazonaws.com --bucket manager-backup-tests-us-east-1 --scope node --prefix {self.base_prefix}/{scylla_node.uuid} --keyspace keyspace1 --table standard1 --sstables-file-list {filename}")
        if res is not None and res.exit_status != 0:
            raise Exception(f"Restore failed: {res.stdout}")

    def restore_and_report(self, label):
        self.set_balancing(False)
        restore_threads = []
        with ExecutionTimer() as lns_timer:
            for node in self.db_cluster.nodes:
                thread = threading.Thread(target=self.restore, args=(node,))
                restore_threads.append(thread)
                thread.start()
            for thread in restore_threads:
                thread.join()

        restore_report = {
            "Size": format_size(sum(self.node_backup_size.values()) / len(self.node_backup_size.values())),
            "Time": int(lns_timer.duration.total_seconds()),
        }

        self.report_to_argus(ManagerReportType.BACKUP, restore_report, "nodetool restore")

        # with ExecutionTimer() as repair_timer:
        #     res = self.db_cluster.nodes[0].run_nodetool("cluster repair --keyspace keyspace1 --table standard1")
        #     if res is not None and res.exit_status != 0:
        #         raise Exception(f"Repair failed: {res.stdout}")
        #
        # restore_report = {
        #     "Size": format_size(sum(self.node_backup_size.values()) / len(self.node_backup_size.values())),
        #     "Time": int(repair_timer.duration.total_seconds()),
        # }
        #
        # self.report_to_argus(ManagerReportType.BACKUP, restore_report, "nodetool repair")

        self.set_balancing(True)

    def set_balancing(self, balancing: bool):
        for node in self.db_cluster.nodes:
            balancing_cmd = build_node_api_command(
                f'/storage_service/tablets/balancing?enabled={"true" if balancing else "false"}', RequestMethods.POST)
            result = node.remoter.run(balancing_cmd, ignore_status=True, verbose=True)

    # actual tests
    def test_just_native_backup_restore(self):
        self.log.info("Executing test_backup_restore_benchmark...")

        # for node in self.db_cluster.nodes:
        #     res = node.remoter.sudo(shell_script_cmd(f"""\
        #     df
        #         """))
        #     print (res.stdout)
        #     assert False

        script = """\
set -eux

JOURNAL_PATH='/var/lib/scylla/systemd-journal'
OVERRIDE_FILE='/etc/systemd/journald.conf.d/override.conf'

echo [+] Creating new journal directory at ${JOURNAL_PATH}
mkdir -p /var/lib/scylla/systemd-journal
chown root:systemd-journal /var/lib/scylla/systemd-journal
chmod 2755 /var/lib/scylla/systemd-journal

echo [+] Backing up existing journal logs - if any
if [ -d /var/log/journal ]; then
    mv /var/log/journal '/var/log/journal.bak.$(date +%s)'
fi

echo [+] Creating symbolic link
ln -s /var/lib/scylla/systemd-journal /var/log/journal

echo [+] Writing journald configuration
mkdir -p '$(dirname /etc/systemd/journald.conf.d/override.conf)'
echo '[Journal]
Storage=persistent
RateLimitInterval=0
RateLimitBurst=0' | tee /etc/systemd/journald.conf.d/override.conf

echo [✓] Journald config written.
systemctl restart systemd-journald
echo [✓] Journald reconfigured and restarted
"""

        for node in self.db_cluster.nodes:
            node.remoter.sudo(shell_script_cmd("""\
            echo '\nobject_storage_endpoints:\n  - name: s3.us-east-1.amazonaws.com\n    port: 443\n    https: true\n    aws_region: us-east-1\n    iam_role_arn: arn:aws:iam::797456418907:instance-profile/qa-scylla-manager-backup-instance-profile\n' >> /etc/scylla/scylla.yaml
                """))
            # res = node.remoter.sudo(shell_script_cmd(script))
            # print (res.stdout)
            node.restart_scylla_server()

        self.log.info("Write data to table")
        self.run_prepare_write_cmd()

        self.log.info("Create and report backup time")

        self.backup_and_report("Native backup")

        self.db_cluster.nodes[0].run_cqlsh(f'TRUNCATE keyspace1.standard1')
        self.db_cluster.nodes[0].run_cqlsh("ALTER TABLE keyspace1.standard1 WITH tombstone_gc = {'mode': 'disabled'};")

        self.restore_and_report("Native restore")

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

        def list_toc_files(bucket, prefix):
            s3 = boto3.client("s3")
            paginator = s3.get_paginator("list_objects_v2")

            toc_files = []

            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith("-big-TOC.txt"):
                        filename = os.path.basename(key)
                        toc_files.append(filename)

            return toc_files

        toc_map = {}  # {node.uuid: [toc files]}

        for node in self.db_cluster.nodes:
            s3_dir = node_directories[uuid.UUID(node.uuid).int % len(self.db_cluster.nodes)]
            toc_list = list_toc_files(
                "manager-backup-tests-permanent-snapshots-us-east-1",
                f"ernest-sct-tests/6TB-tablets-RF3-6node/{s3_dir}"
            )
            toc_map[node.uuid] = toc_list

        def restore(scylla_node: BaseNode, toc_list):
            filename = f'/tmp/{scylla_node.uuid}-sstables.txt'
            with open(filename, "w") as file:
                file.write("\n".join(toc_list))
            scylla_node.remoter.send_files(src=filename, dst=filename)

            s3_dir = node_directories[uuid.UUID(scylla_node.uuid).int % len(self.db_cluster.nodes)]

            self.log.info(f"FOOOOOO starting restore for {scylla_node.host_id}")
            res = scylla_node.run_nodetool(
                f"restore --endpoint s3.us-east-1.amazonaws.com "
                f"--bucket manager-backup-tests-permanent-snapshots-us-east-1 "
                f"--scope all "
                f"--prefix ernest-sct-tests/6TB-tablets-RF3-6node/{s3_dir} "
                f"--keyspace keyspace1 --table standard1 "
                f"--sstables-file-list {filename}"
            )

            self.log.info(f"FOOOOOO restore ended for{scylla_node.host_id}\n{res.stdout}")
            if res is not None and res.exit_status != 0:
                raise Exception(f"Restore failed: {res.stdout}")

        self.set_balancing(False)
        restore_threads = []
        with ExecutionTimer() as lns_timer:
            for node in self.db_cluster.nodes:
                thread = threading.Thread(target=restore, args=(node, toc_map[node.uuid]))
                restore_threads.append(thread)
                thread.start()
            for thread in restore_threads:
                thread.join()

        restore_report = {
            # "Size": format_size(sum(self.node_backup_size.values()) / len(self.node_backup_size.values())),
            "Size": format_size(3000000000),
            "Time": int(lns_timer.duration.total_seconds()),
        }

        self.report_to_argus(ManagerReportType.BACKUP, restore_report, "nodetool restore")

    def test_just_backup_restore(self):

        self.log.info("Write data to table")
        self.run_prepare_write_cmd()

        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = self.db_cluster.get_cluster_manager(force_add=True)
        backup_task = self.create_backup_and_report(mgr_cluster, "`rclone` based backup")

        self.db_cluster.nodes[0].run_cqlsh(f'TRUNCATE keyspace1.standard1')

        self.set_balancing(False)

        self.create_restore_and_report(mgr_cluster, backup_task.get_snapshot_tag(), "`rclone` based restore")

    def test_rclone_backup_restore(self):

        self.log.info("Write data to table")
        self.run_prepare_write_cmd()
        self.set_balancing(False)
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = self.db_cluster.get_cluster_manager(force_add=True)
        backup_task = self.create_backup_and_report(mgr_cluster, "`rclone` based backup")

        self.run_read_stress_and_report(" w/o concurrent backup")

        backup_thread = threading.Thread(target=self.create_backup_and_report,
                                         kwargs={"mgr_cluster": mgr_cluster,
                                                 "label": "`rclone` based backup during R/W stress"})

        read_stress_thread = threading.Thread(target=self.run_read_stress_and_report,
                                              kwargs={"label": " with concurrent `rclone` based backup"})
        backup_thread.start()
        read_stress_thread.start()

        backup_thread.join()
        read_stress_thread.join()

        self.db_cluster.nodes[0].run_cqlsh(f'TRUNCATE keyspace1.standard1')

        self.create_restore_and_report(mgr_cluster, backup_task.get_snapshot_tag(), "`rclone` based restore")

    def test_native_backup_restore(self):
        for node in self.db_cluster.nodes:
            node.remoter.sudo(shell_script_cmd("""\
            echo '\nobject_storage_endpoints:\n  - name: s3.us-east-1.amazonaws.com\n    port: 443\n    https: true\n    aws_region: us-east-1\n    iam_role_arn: arn:aws:iam::797456418907:instance-profile/qa-scylla-manager-backup-instance-profile\n' >> /etc/scylla/scylla.yaml
                """))
            node.restart_scylla_server()

        self.log.info("Write data to table")
        self.run_prepare_write_cmd()

        self.backup_and_report("Native backup")
        self.run_read_stress_and_report(" w/o concurrent native backup")

        self.node_backup_size = {}
        backup_thread = threading.Thread(target=self.backup_and_report,
                                         kwargs={"label": "Native backup during R/W stress"})

        read_stress_thread = threading.Thread(target=self.run_read_stress_and_report,
                                              kwargs={"label": " with concurrent native backup"})
        backup_thread.start()
        read_stress_thread.start()

        backup_thread.join()
        read_stress_thread.join()

        self.db_cluster.nodes[0].run_cqlsh(f'TRUNCATE keyspace1.standard1')
        self.db_cluster.nodes[0].run_cqlsh("ALTER TABLE keyspace1.standard1 WITH tombstone_gc = {'mode': 'disabled'};")

        self.restore_and_report("Native restore")

        self.db_cluster.nodes[0].run_cqlsh("ALTER TABLE keyspace1.standard1 WITH tombstone_gc = {'mode': 'repair'};")

    def test_tablet_aware_restore(self):
        self.log.info("Executing test_tablet_aware_restore...")

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
                                                    AND tablets = {'min_tablet_count': 1024, 'max_tablet_count': 1024}
                                                    AND tombstone_gc = {'mode': 'disabled'};
                                           ''')

        node_names = ["d19d62b8-1c89-11f1-b388-025bfb20f2b9", "d1a065bc-1c89-11f1-92b7-02c21ce218c3",
                      "d1ba103e-1c89-11f1-9eab-027eb0300c8b", "d211556a-1c89-11f1-b1f0-02d6605209f7",
                      "d2868786-1c89-11f1-a55c-02a2fd76cc55", "d3518738-1c89-11f1-8d72-02b6db3c724f"]
        manifests = [
            f"ernest-sct-tests/6TB-tablets-RF3-6node/{node}/manifest.json"
            for node in node_names
        ]
        self.set_balancing(False)
        storage_client = StorageServiceClient(node=self.db_cluster.nodes[0])
        tm_client = RemoteCurlClient(host="localhost:10000", endpoint="task_manager", node=self.db_cluster.nodes[0])
        with ExecutionTimer() as lns_timer:
            tid = storage_client.tablet_aware_restore(ks="keyspace1", cf="standard1", snap="tablet_aware_restore_001",
                                                      endpoint="s3.us-east-1.amazonaws.com",
                                                      bucket="manager-backup-tests-permanent-snapshots-us-east-1",
                                                      manifests=manifests).stdout.strip().strip('"')
            self.log.warn(f"tablet_aware_restore tid: {tid}")
            sleep(30*60)
            # res = self.db_cluster.nodes[0].run_cqlsh("SELECT * FROM system.tablets")
            # self.log.warn(f"tablet_aware_restore - SELECT * FROM system.tablets: {res.stdout}")
            # res = tm_client.run_remoter_curl(method="GET", path=f'wait_task/{tid}', params=None, timeout=2 * 60 * 60)
            # self.log.warn(f"tablet_aware_restore res of task wait: {res}")

        restore_report = {
            # "Size": format_size(sum(self.node_backup_size.values()) / len(self.node_backup_size.values())),
            "Size": format_size(3000000000),
            "Time": int(lns_timer.duration.total_seconds()),
        }

        self.report_to_argus(ManagerReportType.BACKUP, restore_report, "tablet aware restore")
        cql_res = self.db_cluster.nodes[0].run_cqlsh("select count(*) from keyspace1.standard1 BYPASS CACHE")
        self.log.warn(f"tablet_aware_restore - cql select result: {cql_res.stdout}")
        self.log.warn(f"tablet_aware_restore - table rows: {cql_res.current_rows[0].count}")

    def test_create_permanent_backup(self):
        for node in self.db_cluster.nodes:
            node.remoter.sudo(shell_script_cmd("""\
                        echo '\nobject_storage_endpoints:\n  - name: s3.us-east-1.amazonaws.com\n    port: 443\n    https: true\n    aws_region: us-east-1\n    iam_role_arn: arn:aws:iam::797456418907:instance-profile/qa-scylla-manager-backup-instance-profile\n' >> /etc/scylla/scylla.yaml
                            """))
            node.restart_scylla_server()

        self.log.info("Write data to table")
        self.run_prepare_write_cmd()

        def backup(scylla_node: BaseNode):
            scylla_node.run_nodetool("flush")

            snapshot_name = "tablet_aware_restore_001"
            storage_client = StorageServiceClient(node=scylla_node)
            storage_client.snapshot(ks="keyspace1", cf="standard1", snap=snapshot_name).stdout.strip()

            self.log.warn(f"tablet_aware_restore - snapshot taken, now starting backup for {scylla_node.host_id}")
            backup_res = scylla_node.run_nodetool(
                f"backup --endpoint s3.us-east-1.amazonaws.com --bucket manager-backup-tests-permanent-snapshots-us-east-1 --prefix ernest-sct-tests/6TB-tablets-RF3-6node/{scylla_node.uuid} --keyspace keyspace1 --table standard1 --snapshot {snapshot_name}")
            self.log.warn(f"tablet_aware_restore - backup ended for {scylla_node.host_id}\n{backup_res.stdout}")

            if backup_res is not None and backup_res.exit_status != 0:
                raise Exception(f"Backup failed: {backup_res.stdout}")

        self.log.info("Create and report backup time")
        backup_threads = []
        for node in self.db_cluster.nodes:
            thread = threading.Thread(target=backup, args=(node,))
            backup_threads.append(thread)
            thread.start()
        for thread in backup_threads:
            thread.join()
