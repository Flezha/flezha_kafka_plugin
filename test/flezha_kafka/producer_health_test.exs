defmodule FlezhaKafka.ProducerHealthTest do
  @moduledoc """
  Kafka producer health contract (self-healing-control-plane, Phase 7.1).
  Healthy = the worker the table started is alive and not permanently parked; a
  worker whose `last_error` is `{:swap_exhausted, _}` has exhausted its
  in-process broker-swap recovery and is escalated. A wedged stats read errs on
  the side of healthy.

  The tracked process arrives as `:owned_pid`, injected by the table at the one
  site: the table started the worker, so the table knows its pid.
  """
  use ExUnit.Case, async: true

  alias FlezhaKafka.Producer
  alias FlezhaTest.Helper.NodeTestHelper

  defmodule StatsStub do
    use GenServer
    def start(stats), do: GenServer.start(__MODULE__, stats)
    @impl true
    def init(stats), do: {:ok, stats}
    @impl true
    def handle_call(:stats, _from, stats), do: {:reply, stats, stats}
  end

  defp stub(stats) do
    {:ok, pid} = StatsStub.start(stats)
    on_exit(fn -> NodeTestHelper.ensure_stopped(pid) end)
    pid
  end

  test "healthy when the worker is alive and not swap-exhausted" do
    pid = stub(%{last_error: nil, queue_depth: 0, in_flight: 0, dropped_total: 0})
    assert Producer.health(%{owned_pid: pid}) == :healthy
  end

  test "unhealthy when the worker has permanently parked (swap exhausted)" do
    pid = stub(%{last_error: {:swap_exhausted, :no_broker}, queue_depth: 9, in_flight: 0})
    assert Producer.health(%{owned_pid: pid}) == {:unhealthy, {:swap_exhausted, :no_broker}}
  end

  test "unhealthy with :produce_stalled after N consecutive failed batches" do
    pid = stub(%{last_error: :no_broker, consecutive_failures: 3, queue_depth: 5, in_flight: 0})
    assert Producer.health(%{owned_pid: pid}) == {:unhealthy, :produce_stalled}
  end

  test "healthy again once a successful batch resets the consecutive-failure run" do
    pid = stub(%{last_error: nil, consecutive_failures: 0, queue_depth: 0, in_flight: 0})
    assert Producer.health(%{owned_pid: pid}) == :healthy
  end

  test "still healthy below the stall threshold" do
    pid = stub(%{last_error: :no_broker, consecutive_failures: 2, queue_depth: 1, in_flight: 0})
    assert Producer.health(%{owned_pid: pid}) == :healthy
  end

  test "unhealthy when there is no producer pid" do
    assert Producer.health(%{}) == {:unhealthy, :no_producer}
  end

  test "unhealthy when the worker pid is dead" do
    pid = stub(%{last_error: nil})
    GenServer.stop(pid)
    assert Producer.health(%{owned_pid: pid}) == {:unhealthy, :worker_dead}
  end

  test "falls back to :owned_pid when :producer_pid is absent" do
    pid = stub(%{last_error: nil})
    assert Producer.health(%{owned_pid: pid}) == :healthy
  end
end
