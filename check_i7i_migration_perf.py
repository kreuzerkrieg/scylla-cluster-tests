from mgmt_cli_test import ManagerTestFunctionsMixIn
from sdcm.remote import shell_script_cmd


class i7iMigrationPerf(ManagerTestFunctionsMixIn):

    def test_migration(self):
        for node in self.db_cluster.nodes:
            node.remoter.sudo(shell_script_cmd(f"""\
            echo 'stream_io_throughput_mb_per_sec: 0\n' >> /etc/scylla/scylla.yaml
                """))
            node.restart_scylla_server()

        self.log.info("Write data to table")
        self.run_prepare_write_cmd()
        for node in self.db_cluster.nodes:
            node.run_nodetool("compact")
        self.get_stress_results(self.run_stress_thread(self.params.get('stress_cmd')[0], duration=30, stress_num=2))
        self.loaders.nodes[0].remoter.sudo("apt update")
        self.loaders.nodes[0].remoter.sudo("DEBIAN_FRONTEND=noninteractive apt install -y iperf3")
        self.log.info("Start iperf3 server on loader node 1")
        self.loaders.nodes[0].remoter.sudo("nohup iperf3 -s > /tmp/iperf_server.log 2>&1 &")

        self.db_cluster.nodes[0].remoter.sudo("apt update")
        self.db_cluster.nodes[0].remoter.sudo("DEBIAN_FRONTEND=noninteractive apt install -y iperf3")
        self.log.info("Start iperf3 client on scylla node 1")
        self.db_cluster.nodes[0].remoter.sudo(f"iperf3 -c {self.loaders.nodes[0].ip_address} -t 3600 -P 10",
                                              timeout=3700, ignore_status=True)

        # sleep(120)  # just create a clear separation between before and after migration on the graphs

        new_nodes = self.db_cluster.add_nodes(count=1, enable_auto_bootstrap=True)
        self.db_cluster.wait_for_init(node_list=new_nodes)
        self.monitors.reconfigure_scylla_monitoring()
        for node in new_nodes:
            node.remoter.sudo(shell_script_cmd(f"""\
            echo 'stream_io_throughput_mb_per_sec: 0\n' >> /etc/scylla/scylla.yaml
                """))
            node.restart_scylla_server()

        self.get_stress_results(self.run_stress_thread(self.params.get('stress_cmd')[0], duration=30, stress_num=2))
