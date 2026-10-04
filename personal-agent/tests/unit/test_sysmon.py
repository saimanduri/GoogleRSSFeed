from pa_gateway.sysmon import aggregate

E = r"\GPU Engine(pid_%d_luid_%s_phys_0_eng_%d_engtype_%s)\Utilization Percentage"
A = "0x00000000_0x00012592"
B = "0x00000000_0x00013a2a"


def test_adapter_load_is_the_busiest_engine_type_summed_over_processes():
    eng = {E % (1, A, 0, "3D"): 30, E % (2, A, 0, "3D"): 25, E % (2, A, 3, "Copy"): 10, E % (3, B, 0, "Compute_0"): 80}
    mem = {r"\GPU Adapter Memory(luid_%s_phys_0)\Dedicated Usage" % A: 2048 * 1024 * 1024,
           r"\GPU Adapter Memory(luid_%s_phys_0)\Dedicated Usage" % B: 512 * 1024 * 1024}
    r = aggregate(eng, mem)
    assert r == {"util": 80.0, "vram_mb": 2048, "adapters": 2}


def test_caps_at_100_and_handles_empty():
    assert aggregate({E % (1, A, 0, "3D"): 70, E % (2, A, 0, "3D"): 70}, {})["util"] == 100.0
    assert aggregate({}, {}) == {"util": 0.0, "vram_mb": 0, "adapters": 0}
