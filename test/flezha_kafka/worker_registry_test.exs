defmodule FlezhaKafka.WorkerRegistryTest do
  alias FlezhaKafka.WorkerRegistry
  alias FlezhaKafka.Test.Helpers

  # async: false: registered singletons.

  use ExUnit.Case, async: false

  @hosts_a [{"kafka-a", 9092}]
  @hosts_b [{"kafka-b", 9092}]

  setup do
    test_pid = self()

    starter = fn identity ->
      pid =
        spawn(fn ->
          receive do
            :stop -> :ok
          end
        end)

      send(test_pid, {:starter_called, identity, pid})
      {:ok, pid}
    end

    stopper = fn pid ->
      send(test_pid, {:stopper_called, pid})
      send(pid, :stop)
      :ok
    end

    {:ok, registry} =
      start_supervised(
        {WorkerRegistry,
         name: :"#{__MODULE__}.#{System.unique_integer([:positive])}",
         starter: starter,
         stopper: stopper,
         linger_ms: 50}
      )

    {:ok, registry: registry}
  end

  describe "single shared worker per identity" do
    test "five plugins against the same identity share one worker", %{registry: reg} do
      identity = %{hosts: @hosts_a}

      tokens =
        for _i <- 1..5 do
          assert {:ok, pid, ref} = WorkerRegistry.acquire(identity, reg)
          assert is_pid(pid)
          {pid, ref}
        end

      assert_receive {:starter_called, _identity, worker_pid}
      refute_received {:starter_called, _, _}

      pids = tokens |> Enum.map(&elem(&1, 0)) |> Enum.uniq()
      assert pids == [worker_pid]

      summary = WorkerRegistry.inspect_state(reg)
      assert map_size(summary) == 1
      [{_key, entry}] = Map.to_list(summary)
      assert entry.refs == 5
      assert entry.linger? == false
    end

    test "tearing down N-1 leaves the worker running; tearing down the last triggers linger stop",
         %{registry: reg} do
      identity = %{hosts: @hosts_a}

      tokens =
        for _i <- 1..3 do
          assert {:ok, _, ref} = WorkerRegistry.acquire(identity, reg)
          ref
        end

      assert_receive {:starter_called, _identity, worker_pid}

      [first | rest] = tokens

      Enum.each(rest, fn ref -> :ok = WorkerRegistry.release(ref, reg) end)

      summary = WorkerRegistry.inspect_state(reg)
      [{_key, entry}] = Map.to_list(summary)
      assert entry.refs == 1
      assert entry.linger? == false

      refute_received {:stopper_called, ^worker_pid}

      :ok = WorkerRegistry.release(first, reg)

      summary = WorkerRegistry.inspect_state(reg)
      [{_key, entry}] = Map.to_list(summary)
      assert entry.refs == 0
      assert entry.linger? == true

      assert_receive {:stopper_called, ^worker_pid}, 500
      assert WorkerRegistry.inspect_state(reg) == %{}
    end

    test "release within linger window reuses the same worker pid", %{registry: reg} do
      identity = %{hosts: @hosts_a}

      assert {:ok, pid1, ref1} = WorkerRegistry.acquire(identity, reg)
      assert_receive {:starter_called, _, _}

      :ok = WorkerRegistry.release(ref1, reg)

      assert {:ok, pid2, _ref2} = WorkerRegistry.acquire(identity, reg)
      assert pid2 == pid1
      refute_received {:starter_called, _, _}
      refute_received {:stopper_called, _}
    end

    test "implicit release when caller dies", %{registry: reg} do
      identity = %{hosts: @hosts_a}
      test_pid = self()

      caller =
        spawn(fn ->
          assert {:ok, _pid, ref} = WorkerRegistry.acquire(identity, reg)
          send(test_pid, {:acquired, ref})

          receive do
            :exit -> :ok
          end
        end)

      assert_receive {:acquired, _ref}
      assert_receive {:starter_called, _, worker_pid}

      summary = WorkerRegistry.inspect_state(reg)
      [{_key, entry}] = Map.to_list(summary)
      assert entry.refs == 1

      Process.exit(caller, :kill)

      assert_receive {:stopper_called, ^worker_pid}, 500
      assert WorkerRegistry.inspect_state(reg) == %{}
    end
  end

  describe "distinct identities get distinct workers" do
    test "two identities yield two workers", %{registry: reg} do
      assert {:ok, pid_a, _} = WorkerRegistry.acquire(%{hosts: @hosts_a}, reg)
      assert {:ok, pid_b, _} = WorkerRegistry.acquire(%{hosts: @hosts_b}, reg)

      assert pid_a != pid_b
      assert map_size(WorkerRegistry.inspect_state(reg)) == 2
    end

    test "host order does not affect identity", %{registry: reg} do
      assert {:ok, pid1, _} =
               WorkerRegistry.acquire(
                 %{hosts: [{"k1", 9092}, {"k2", 9092}]},
                 reg
               )

      assert {:ok, pid2, _} =
               WorkerRegistry.acquire(
                 %{hosts: [{"k2", 9092}, {"k1", 9092}]},
                 reg
               )

      assert pid1 == pid2
      assert map_size(WorkerRegistry.inspect_state(reg)) == 1
    end

    test "ssl_options difference yields distinct workers", %{registry: reg} do
      assert {:ok, pid1, _} =
               WorkerRegistry.acquire(
                 %{hosts: @hosts_a, ssl_options: [verify: :verify_none]},
                 reg
               )

      assert {:ok, pid2, _} =
               WorkerRegistry.acquire(
                 %{hosts: @hosts_a, ssl_options: [verify: :verify_peer]},
                 reg
               )

      assert pid1 != pid2
    end
  end

  describe "worker death" do
    test "worker DOWN clears the entry; next acquire starts fresh worker", %{registry: reg} do
      identity = %{hosts: @hosts_a}

      assert {:ok, pid1, _ref1} = WorkerRegistry.acquire(identity, reg)
      assert_receive {:starter_called, _, _}

      Process.exit(pid1, :kill)

      assert Helpers.wait_until(fn -> WorkerRegistry.inspect_state(reg) == %{} end, 500, 5)

      assert {:ok, pid2, _ref2} = WorkerRegistry.acquire(identity, reg)
      assert_receive {:starter_called, _, _}
      assert pid2 != pid1
    end
  end

  describe "starter failure" do
    test "acquire returns the starter's error", %{registry: _reg} do
      starter = fn _ -> {:error, :no_brokers_available} end

      {:ok, reg} =
        start_supervised(
          {WorkerRegistry,
           name: :"#{__MODULE__}.starter_fail.#{System.unique_integer([:positive])}",
           starter: starter,
           stopper: fn _ -> :ok end,
           linger_ms: 50},
          id: :"starter_fail_#{System.unique_integer([:positive])}"
        )

      assert {:error, :no_brokers_available} = WorkerRegistry.acquire(%{hosts: @hosts_a}, reg)
      assert WorkerRegistry.inspect_state(reg) == %{}
    end
  end

  describe "release of stale ref" do
    test "release on a unknown ref is a no-op", %{registry: reg} do
      stale_ref = {{[{"unknown", 9092}], 0, 0}, make_ref()}
      assert :ok = WorkerRegistry.release(stale_ref, reg)
    end
  end

  describe "caller tracking on entries" do
    test "entry.callers grows on acquire and shrinks on release", %{registry: reg} do
      identity = %{hosts: @hosts_a}

      assert {:ok, _pid_a, ref_a} = WorkerRegistry.acquire(identity, reg)
      assert {:ok, _pid_b, ref_b} = WorkerRegistry.acquire(identity, reg)
      assert {:ok, _pid_c, ref_c} = WorkerRegistry.acquire(identity, reg)

      summary = WorkerRegistry.inspect_state(reg)
      [{_key, entry}] = Map.to_list(summary)
      assert entry.callers == 3
      assert entry.refs == 3

      :ok = WorkerRegistry.release(ref_b, reg)

      summary = WorkerRegistry.inspect_state(reg)
      [{_key, entry}] = Map.to_list(summary)
      assert entry.callers == 2
      assert entry.refs == 2

      :ok = WorkerRegistry.release(ref_a, reg)
      :ok = WorkerRegistry.release(ref_c, reg)
    end
  end

  describe "proactive death notification" do
    test "every active caller receives {:kafka_worker_dead, ref, reason} on DOWN", %{
      registry: reg
    } do
      identity = %{hosts: @hosts_a}

      test_pid = self()

      caller_a =
        spawn(fn ->
          {:ok, _pid, ref} = WorkerRegistry.acquire(identity, reg)
          send(test_pid, {:caller_a_ref, ref})

          receive do
            msg -> send(test_pid, {:caller_a_msg, msg})
          end
        end)

      caller_b =
        spawn(fn ->
          {:ok, _pid, ref} = WorkerRegistry.acquire(identity, reg)
          send(test_pid, {:caller_b_ref, ref})

          receive do
            msg -> send(test_pid, {:caller_b_msg, msg})
          end
        end)

      assert_receive {:caller_a_ref, ref_a}
      assert_receive {:caller_b_ref, ref_b}
      assert_receive {:starter_called, _, worker_pid}

      summary = WorkerRegistry.inspect_state(reg)
      [{_key, entry}] = Map.to_list(summary)
      assert entry.callers == 2

      Process.exit(worker_pid, :kill)

      assert_receive {:caller_a_msg, {:kafka_worker_dead, ^ref_a, :killed}}, 500
      assert_receive {:caller_b_msg, {:kafka_worker_dead, ^ref_b, :killed}}, 500

      _ = caller_a
      _ = caller_b
      :ok
    end
  end

  describe "swap/2 and pending_refs" do
    test "three callers swap after worker death; entry refs is preserved (path A)" do
      identity = %{hosts: @hosts_a}
      test_pid = self()

      starter = fn _identity ->
        pid =
          spawn(fn ->
            receive do
              :stop -> :ok
            end
          end)

        send(test_pid, {:starter_called, pid})
        {:ok, pid}
      end

      stopper = fn pid ->
        send(pid, :stop)
        send(test_pid, {:stopper_called, pid})
        :ok
      end

      {:ok, reg} =
        start_supervised(
          {WorkerRegistry,
           name: :"#{__MODULE__}.swap_path_a.#{System.unique_integer([:positive])}",
           starter: starter,
           stopper: stopper,
           linger_ms: 1_000},
          id: :"swap_path_a_#{System.unique_integer([:positive])}"
        )

      # Three concurrent acquires from three carriers; each holds its own caller monitor.
      caller_pids =
        for _i <- 1..3 do
          parent = self()

          spawn(fn ->
            {:ok, _, ref} = WorkerRegistry.acquire(identity, reg)
            send(parent, {:acquired, ref})

            # The registry announces every DOWN it has processed. Forwarding it
            # is the barrier: until all three have arrived, a swap can still be
            # answered off the entry the dead worker left behind.
            receive do
              {:kafka_worker_dead, ^ref, :killed} -> send(parent, {:dead, ref})
            end

            receive do
              :swap ->
                {:ok, _, new_ref} = WorkerRegistry.swap(ref, reg)
                send(parent, {:swapped, new_ref})

                receive do
                  :stop -> :ok
                end
            end
          end)
        end

      refs =
        for _ <- 1..3 do
          assert_receive {:acquired, ref}
          ref
        end

      assert_receive {:starter_called, worker_pid}

      Process.exit(worker_pid, :kill)

      for ref <- refs do
        assert_receive {:dead, ^ref}, 500
      end

      # The first swap triggers a new worker; the next two attach to that worker.
      Enum.each(caller_pids, fn pid -> send(pid, :swap) end)

      for _ <- 1..3 do
        assert_receive {:swapped, _new_ref}, 1_000
      end

      assert_receive {:starter_called, _new_worker_pid}, 100

      summary = WorkerRegistry.inspect_state(reg)
      [{_key, entry}] = Map.to_list(summary)
      assert entry.refs == 3
      assert entry.callers == 3

      Enum.each(caller_pids, fn pid -> send(pid, :stop) end)
    end

    test "swap arrives after linger expires: error :no_pending_swap (path B)" do
      identity = %{hosts: @hosts_a}
      test_pid = self()

      starter = fn _identity ->
        pid =
          spawn(fn ->
            receive do
              :stop -> :ok
            end
          end)

        send(test_pid, {:starter_called, pid})
        {:ok, pid}
      end

      {:ok, reg} =
        start_supervised(
          {WorkerRegistry,
           name: :"#{__MODULE__}.swap_path_b.#{System.unique_integer([:positive])}",
           starter: starter,
           stopper: fn _ -> :ok end,
           linger_ms: 30},
          id: :"swap_path_b_#{System.unique_integer([:positive])}"
        )

      parent = self()

      caller =
        spawn(fn ->
          {:ok, _, ref} = WorkerRegistry.acquire(identity, reg)
          send(parent, {:acquired, ref})

          receive do
            :swap_now ->
              send(parent, {:swap_result, WorkerRegistry.swap(ref, reg)})
          end
        end)

      assert_receive {:acquired, _}
      assert_receive {:starter_called, worker_pid}

      Process.exit(worker_pid, :kill)
      # The death is handled (its monitor gone) and the pending refs it
      # snapshotted are dropped once the linger lapses.
      assert Helpers.wait_until(
               fn ->
                 state = :sys.get_state(reg)
                 state.worker_monitors == %{} and state.pending_refs == %{}
               end,
               1_000,
               5
             )

      send(caller, :swap_now)
      assert_receive {:swap_result, {:error, :no_pending_swap}}
    end
  end
end
